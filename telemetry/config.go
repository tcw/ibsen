package telemetry

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/propagation"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"google.golang.org/grpc"
	"sync"
	"time"
)

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
	metricClient := otlpmetricgrpc.NewClient(
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint(OTELExporterAddr),
	)
	metricExp, err := otlpmetric.New(ctx, metricClient)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create the collector metric exporter")
		return nil, err
	}

	pusher := controller.New(
		processor.NewFactory(
			simple.NewWithHistogramDistribution(),
			metricExp,
		),
		controller.WithExporter(metricExp),
		controller.WithCollectPeriod(2*time.Second),
	)
	global.SetMeterProvider(pusher)

	err = pusher.Start(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to start metric pusher")
		return nil, err
	}

	traceClient := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(OTELExporterAddr),
		otlptracegrpc.WithDialOption(grpc.WithBlock()))
	traceExp, err := otlptrace.New(ctx, traceClient)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to create the collector trace exporter")
		return nil, err
	}

	res, err := resource.New(ctx,
		//resource.WithFromEnv(),
		//resource.WithProcess(),
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
		if err := pusher.Stop(cxt); err != nil {
			otel.Handle(err)
		}
	}, nil
}
