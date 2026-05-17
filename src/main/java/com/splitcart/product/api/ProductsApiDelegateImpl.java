package com.splitcart.product.api;

import com.splitcart.product.model.Availability;
import com.splitcart.product.model.Product;
import com.splitcart.product.service.ProductService;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
  implementar los metodos listProducts, getProductById y getAvailability usando el servicio ProductService
 */
@Component
@RequiredArgsConstructor
public class ProductsApiDelegateImpl  {

  private final ProductService service;

  public Mono<ResponseEntity<Flux<Product>>> listProducts(
      String category, BigDecimal maxPrice, Integer page, Integer size, ServerWebExchange exchange) {
    return null;
  }

  public Mono<ResponseEntity<Product>> getProductById(String id, ServerWebExchange exchange) {
    return null;
  }

  public Mono<ResponseEntity<Flux<Availability>>> getAvailability(
      String ids, ServerWebExchange exchange) {
    return null;
  }
}
