package com.example.camel.config;

import com.example.camel.model.Owner;
import com.example.camel.model.Tenant;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.*;
import org.apache.camel.builder.ExchangeBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@Slf4j
public class DynamicRouter {

  private final ProducerTemplate producerTemplate;

  private final CamelContext camelContext;

  public DynamicRouter(ProducerTemplate producerTemplate, CamelContext camelContext) {
    this.producerTemplate = producerTemplate;
    this.camelContext = camelContext;
  }

  @Handler
  public String route(Exchange exchange, @Header(Exchange.SLIP_ENDPOINT) String previousEndpoint) {
    return whereToGo(exchange);
  }

  private String whereToGo(Exchange exchange) {

    log.info("Note: Tenant Id created in previous endpoint(SaveDB)");

    Tenant tenant = exchange.getIn().getBody(Tenant.class);
    UUID id = tenant.getId();
    List<Owner> owners = tenant.getOwners();

    Map<String, Object> headers = exchange.getIn().getHeaders();
    headers.put(
        "AGGREGATOR",
        id); // We are using tenant Id for Aggregation So, we must make sure this id is passed along
    // in all headers
    headers.put("TOTAL_OWNERS_COUNT", owners.size());

    for (Owner owner : owners) {

      Map<String, Object> headers_new = new HashMap<>();
      headers_new.putAll(headers);
      headers_new.put("OWNER_ID", owner.getOwnerId());

      Exchange newExchange = ExchangeBuilder.anExchange(camelContext).withBody(tenant).build();

      newExchange.getIn().setHeaders(headers_new);
      // newExchange.getOut().setHeaders(headers_new);

      producerTemplate.asyncSend("direct:TENANT", newExchange);
    }

    return null;
  }
}
