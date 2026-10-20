// main starts the HTTP and gRPC payment service of the CBL platform.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	paymentv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/payment/v1"

	"github.com/ZMichael3012/cbl-kit/grpcmw"
	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/inbox/kafkaconsumer"
	"github.com/ZMichael3012/cbl-kit/outbox"
	"github.com/ZMichael3012/cbl-kit/outbox/kafkapub"
	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-payment/internal/connectorclient"
	"github.com/cbl/cbl-payment/internal/domain"
	"github.com/cbl/cbl-payment/internal/quoteclient"
	"github.com/cbl/cbl-payment/internal/storage"
	"github.com/cbl/cbl-payment/internal/transport/grpcserver"
	"github.com/cbl/cbl-payment/internal/usecase"
)

// main brings up the service gRPC server, the administrative HTTP server and the metrics server.
func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Error("PG_DSN is required")
		return
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Error("connect postgres failed", zap.Error(err))
		return
	}

	// The pool is bounded following the ledger pattern (T-LED-04): the consumer
	// and gRPC share connections, a typo in env must not exhaust max_connections.
	sqlDB, err := db.DB()
	if err != nil {
		log.Error("resolve sql db failed", zap.Error(err))
		return
	}
	maxOpen := min(getenvInt("DB_MAX_OPEN_CONNS", 20), 100)
	maxIdle := min(getenvInt("DB_MAX_IDLE_CONNS", 10), maxOpen)
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	log.Info("postgres pool configured", zap.Int("max_open", maxOpen), zap.Int("max_idle", maxIdle))

	httpAddr := getenv("HTTP_ADDR", ":8030")
	grpcAddr := getenv("GRPC_ADDR", ":9030")
	metricsAddr := getenv("METRICS_ADDR", ":9230")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	migrationsDir := getenv("MIGRATIONS_DIR", "./migrations")
	if err := storagex.Migrate(ctx, db, migrationsDir); err != nil {
		log.Error("apply migrations failed", zap.String("dir", migrationsDir), zap.Error(err))
		return
	}

	repo := storage.NewRepository(db, storage.WithEventSink(usecase.NewPaymentEventSink(log)))
	underBPS, bpsErr := parseToleranceBPS(os.Getenv("PAYMENT_UNDERPAID_TOLERANCE_BPS"))
	if bpsErr != nil {
		log.Error("parse PAYMENT_UNDERPAID_TOLERANCE_BPS failed", zap.Error(bpsErr))
		return
	}
	overBPS, bpsErr := parseToleranceBPS(os.Getenv("PAYMENT_OVERPAID_TOLERANCE_BPS"))
	if bpsErr != nil {
		log.Error("parse PAYMENT_OVERPAID_TOLERANCE_BPS failed", zap.Error(bpsErr))
		return
	}
	policy, policyErr := domain.ParseAmountPolicy(os.Getenv("PAYMENT_CONFIRMATION_POLICY"), underBPS, overBPS)
	if policyErr != nil {
		log.Error("parse PAYMENT_CONFIRMATION_POLICY failed", zap.Error(policyErr))
		return
	}
	log.Info("payment amount policy configured", zap.String("policy", policy.Describe()))

	janitorInterval, durationErr := parseDurationEnv("PAYMENT_JANITOR_INTERVAL", usecase.DefaultJanitorInterval, time.Second, true)
	if durationErr != nil {
		log.Error("parse PAYMENT_JANITOR_INTERVAL failed", zap.Error(durationErr))
		return
	}
	creatingTimeout, durationErr := parseDurationEnv("PAYMENT_CREATING_TIMEOUT", usecase.DefaultCreatingTimeout, time.Minute, false)
	if durationErr != nil {
		log.Error("parse PAYMENT_CREATING_TIMEOUT failed", zap.Error(durationErr))
		return
	}
	stuckTimeout, durationErr := parseDurationEnv("PAYMENT_STUCK_TIMEOUT", usecase.DefaultStuckTimeout, time.Minute, false)
	if durationErr != nil {
		log.Error("parse PAYMENT_STUCK_TIMEOUT failed", zap.Error(durationErr))
		return
	}
	janitorBatch, batchErr := parseBatchEnv("PAYMENT_JANITOR_BATCH", usecase.DefaultJanitorBatch)
	if batchErr != nil {
		log.Error("parse PAYMENT_JANITOR_BATCH failed", zap.Error(batchErr))
		return
	}
	if janitorInterval > 0 {
		janitor, janitorErr := usecase.NewJanitor(repo, log, usecase.JanitorConfig{
			Interval:      janitorInterval,
			Batch:         janitorBatch,
			CreatingAfter: creatingTimeout,
			StuckAfter:    stuckTimeout,
		})
		if janitorErr != nil {
			log.Error("payment janitor configuration failed", zap.Error(janitorErr))
			return
		}
		go janitor.Run(ctx)
		log.Info("payment janitor started",
			zap.Duration("interval", janitorInterval),
			zap.Int("batch", janitorBatch),
			zap.Duration("creating_timeout", creatingTimeout),
			zap.Duration("stuck_timeout", stuckTimeout))
	} else {
		log.Warn("payment janitor disabled (PAYMENT_JANITOR_INTERVAL=0)")
	}

	// Creation flow (4.2): the quote and connector clients are wired when both
	// addresses are configured; otherwise CreatePayment answers Unavailable
	// fail-closed and the reads keep working.
	var creation *usecase.PaymentCreation
	quoteAddr := strings.TrimSpace(os.Getenv("QUOTE_GRPC_ADDR"))
	connectorAddr := strings.TrimSpace(os.Getenv("CONNECTOR_GRPC_ADDR"))
	if quoteAddr != "" && connectorAddr != "" {
		quoteClient, dialErr := quoteclient.Dial(quoteAddr)
		if dialErr != nil {
			log.Error("quote client failed", zap.Error(dialErr))
			return
		}
		defer func() { _ = quoteClient.Close() }()
		connectorClient, dialErr := connectorclient.Dial(connectorAddr)
		if dialErr != nil {
			log.Error("connector client failed", zap.Error(dialErr))
			return
		}
		defer func() { _ = connectorClient.Close() }()
		assetScales, scaleErr := parseAssetScales(os.Getenv("PAYMENT_ASSET_DECIMALS"))
		if scaleErr != nil {
			log.Error("parse PAYMENT_ASSET_DECIMALS failed", zap.Error(scaleErr))
			return
		}
		creation, err = usecase.NewPaymentCreation(repo, quoteClient, connectorClient, usecase.CreationConfig{
			AssetScales: assetScales,
			Log:         log,
		})
		if err != nil {
			log.Error("payment creation configuration failed", zap.Error(err))
			return
		}
		log.Info("payment creation wired",
			zap.String("quote", quoteAddr),
			zap.String("connector", connectorAddr),
			zap.Int("asset_scales", len(assetScales)))
	} else {
		log.Warn("payment creation is disabled: QUOTE_GRPC_ADDR and CONNECTOR_GRPC_ADDR are required")
	}
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		brokerList := splitCSV(brokers)
		publisher, pubErr := kafkapub.New(brokerList)
		if pubErr != nil {
			log.Error("kafka publisher failed", zap.Error(pubErr))
			return
		}
		defer publisher.Close()

		relay := outbox.NewRelay(db, publisher, log)
		go func() {
			if runErr := relay.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("outbox relay stopped", zap.Error(runErr))
			}
		}()

		group := getenv("KAFKA_CONSUMER_GROUP", usecase.ConsumerGroupCanonical)
		consumer := usecase.NewCanonicalConsumer(repo, log, usecase.WithAmountPolicy(policy))
		processor := inbox.NewProcessor(group, db, inbox.NewGuard(), consumer, publisher,
			inbox.NewRouter(usecase.TopicGatewayEventsCanonical, 3), log)
		topics := []string{
			usecase.TopicGatewayEventsCanonical,
			usecase.TopicGatewayEventsCanonical + inbox.RetrySuffix5s,
			usecase.TopicGatewayEventsCanonical + inbox.RetrySuffix1m,
			usecase.TopicGatewayEventsCanonical + inbox.RetrySuffix10m,
		}
		runner, runErr := kafkaconsumer.New(brokerList, group, topics, processor, log)
		if runErr != nil {
			log.Error("canonical consumer failed", zap.Error(runErr))
			return
		}
		defer runner.Close()
		go func() {
			if consumeErr := runner.Run(ctx); consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
				log.Error("canonical consumer stopped", zap.Error(consumeErr))
			}
		}()
		log.Info("canonical consumer started", zap.String("group", group))
	} else {
		log.Warn("KAFKA_BROKERS is empty: canonical consumer and outbox relay disabled, payment events accumulate in outbox")
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	httpSrv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<20),
		grpc.UnaryInterceptor(grpcmw.UnaryServer(log)),
	)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	paymentv1.RegisterPaymentServiceServer(grpcSrv, grpcserver.NewPayment(usecase.NewPaymentQuery(repo), creation))
	reflection.Register(grpcSrv)

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("grpc listen failed", zap.String("addr", grpcAddr), zap.Error(err))
		return
	}

	go func() {
		log.Info("http server started", zap.String("addr", httpAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", metricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("grpc server started", zap.String("addr", grpcAddr))
		if serveErr := grpcSrv.Serve(grpcListener); serveErr != nil {
			log.Error("grpc server failed", zap.Error(serveErr))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	grpcSrv.GracefulStop()
	log.Info("shutdown finished")
}

// metricsHandler serves a metrics stub until task 0.7.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# metrics endpoint placeholder (task 0.7)")
	})
	return mux
}

// getenv returns the environment variable value or the fallback value.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getenvInt returns a positive integer from the environment variable
// or the fallback value.
func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// parseToleranceBPS parses a tolerance in basis points: an empty value means 0,
// an invalid value or one outside the range 0 to 10000 is an error: a money
// decision must not silently degrade because of a configuration typo.
func parseToleranceBPS(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 || value > 10000 {
		return 0, fmt.Errorf("invalid bps %q", raw)
	}
	return value, nil
}

// parseDurationEnv parses a duration from the environment variable: an empty
// value means the fallback; an invalid, negative or below-minimum value is an
// error; zero is allowed only when allowZero is set (janitor interval:
// 0 means disabled). The minimums protect against typos like
// PAYMENT_CREATING_TIMEOUT=1s that would close live 4.2 creations in bulk.
func parseDurationEnv(key string, fallback, minimum time.Duration, allowZero bool) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	if allowZero && value == 0 {
		return 0, nil
	}
	if value < minimum {
		return 0, fmt.Errorf("invalid %s %q: minimum %s", key, raw, minimum)
	}
	return value, nil
}

// parseBatchEnv parses the janitor batch size: an empty value means the
// fallback; an invalid or non-positive value is an error (the ceiling is
// validated by usecase.NewJanitor).
func parseBatchEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid %s %q", key, raw)
	}
	return value, nil
}

// splitCSV parses a comma-separated list of values from the environment
// variable, dropping empty elements and surrounding whitespace.
func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

// parseAssetScales parses the PAYMENT_ASSET_DECIMALS CSV (ASSET:SCALE): the
// pay asset scales of the creation flow (4.2). An unknown asset fails closed
// at request time and an empty or malformed configuration refuses startup.
func parseAssetScales(raw string) (map[string]uint32, error) {
	scales := make(map[string]uint32)
	for _, item := range splitCSV(raw) {
		asset, scaleRaw, found := strings.Cut(item, ":")
		asset = strings.ToUpper(strings.TrimSpace(asset))
		if !found || asset == "" {
			return nil, fmt.Errorf("payment: invalid asset scale %q", item)
		}
		scale, err := strconv.Atoi(strings.TrimSpace(scaleRaw))
		if err != nil || scale < 0 || scale > 18 {
			return nil, fmt.Errorf("payment: invalid asset scale %q", item)
		}
		if _, duplicate := scales[asset]; duplicate {
			return nil, fmt.Errorf("payment: duplicate asset scale %q", asset)
		}
		scales[asset] = uint32(scale)
	}
	if len(scales) == 0 {
		return nil, errors.New("payment: no asset scales configured")
	}
	return scales, nil
}
