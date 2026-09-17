package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// collectPeriod is how often metrics are pushed to the collector.
const collectPeriod = 2 * time.Second

// ConnectToOTELExporter sets up the OTLP providers and keeps them alive until wg says the
// server has stopped serving, then flushes and shuts them down.
//
// The exporters connect lazily: the trace client used to be given grpc.WithBlock(), so
// initProvider failed and this loop retried every ten seconds while the collector was down.
// Both that dial option and its grpc counterpart are no-ops now, and the OTLP exporters
// buffer and retry on their own, so setup succeeds even with no collector listening and the
// retry here only covers genuine configuration failures.
func ConnectToOTELExporter(wg *sync.WaitGroup, OTELExporterAddr string) {
	for {
		provider, err := initProvider(OTELExporterAddr)
		if err != nil {
			time.Sleep(time.Second * 10)
			continue
		}
		wg.Wait()
		provider()
		break
	}
}

func initProvider(OTELExporterAddr string) (func(), error) {
	ctx := context.Background()

	log.Info().Msgf("Connecting to OTEL exporter %s ...", OTELExporterAddr)
	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(OTELExporterAddr),
	)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create the collector metric exporter")
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("ibsen"),
		),
	)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create the resource")
		return nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(collectPeriod))),
	)
	otel.SetMeterProvider(meterProvider)

	traceClient := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(OTELExporterAddr))
	traceExp, err := otlptrace.New(ctx, traceClient)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create the collector trace exporter")
		return nil, err
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExp)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetTracerProvider(tracerProvider)

	log.Info().Msgf("Connection to OTEL exporter %s established", OTELExporterAddr)
	return func() {
		cxt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := traceExp.Shutdown(cxt); err != nil {
			otel.Handle(err)
		}
		// pushes any last exports to the receiver
		if err := meterProvider.Shutdown(cxt); err != nil {
			otel.Handle(err)
		}
	}, nil
}
