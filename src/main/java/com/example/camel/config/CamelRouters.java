package com.example.camel.config;

import com.example.camel.model.*;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class CamelRouters extends RouteBuilder {

  private final RouteSurprises routeSurprises;

  private final SaveToDB saveToDB;

  private final DynamicRouter dynamicRouter;

  protected final OwnerContactProcessor ownerContactProcessor;

  private final LoadOwnerConfigs loadOwnerConfigs;

  private final URLTransformer urlTransformer;

  private final PayloadTransformer payloadTransformer;

  public CamelRouters(
      RouteSurprises routeSurprises,
      SaveToDB saveToDB,
      DynamicRouter dynamicRouter,
      OwnerContactProcessor ownerContactProcessor,
      LoadOwnerConfigs loadOwnerConfigs,
      URLTransformer urlTransformer,
      PayloadTransformer payloadTransformer) {
    this.routeSurprises = routeSurprises;
    this.saveToDB = saveToDB;
    this.dynamicRouter = dynamicRouter;
    this.ownerContactProcessor = ownerContactProcessor;
    this.loadOwnerConfigs = loadOwnerConfigs;
    this.urlTransformer = urlTransformer;
    this.payloadTransformer = payloadTransformer;
  }

  @PostConstruct
  public void init() {
    log.info("Routes Loaded");
  }

  @Override
  public void configure() throws Exception {

    // onException(ExampleException.class).handled(true).bean(routeSurprises).stop();

    from("direct:REST").process(saveToDB).dynamicRouter(method(dynamicRouter));

    from("direct:TENANT")
        .threads(4, 8, "[*G*T]")
        .process(loadOwnerConfigs)
        .process(urlTransformer)
        .process(payloadTransformer)
        .aggregate(header("AGGREGATOR"), new ConcurrentAggregationStrategy())
        .completionSize(header("TOTAL_OWNERS_COUNT"))
        .completionTimeout(1000)
        .log("${header.AGGREGATOR.size()} out of ${header.TOTAL_OWNERS_COUNT.size()} completed")
        .process(ownerContactProcessor)
        .log("Dynamic Route processing completed");
  }
}
