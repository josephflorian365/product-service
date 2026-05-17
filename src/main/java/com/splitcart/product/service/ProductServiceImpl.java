package com.splitcart.product.service;

import com.splitcart.product.model.Availability;
import com.splitcart.product.model.Product;
import com.splitcart.product.repo.ProductRepository;
import java.math.BigDecimal;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
    implementar los metodos list, getById y availability usando el repositorio ProductRepository
 */
@Service
@RequiredArgsConstructor
public class ProductServiceImpl implements ProductService {

    private final ProductRepository repository;

    @Override
    public Flux<Product> list(String category, BigDecimal maxPrice, int page, int size) {
        // Implementar paginación y filtros
        Flux<Product> products = Flux.empty();

        if (category != null && maxPrice != null) {
            products = repository.findByCategoryAndPriceLessThanEqual(category, maxPrice.doubleValue());
        } else if (category != null) {
            products = repository.findByCategory(category);
        } else if (maxPrice != null) {
            products = repository.findByPriceLessThanEqual(maxPrice.doubleValue());
        } else {
            products = repository.findAll();
        }

        // Aplicar paginación simple (skip y take)
        return products.skip((long) page * size).take(size);
    }

    @Override
    public Mono<Product> getById(String id) {
        return repository.findById(id);
    }

    @Override
    public Flux<Availability> availability(List<String> skus) {
        // Para simplificar, devolver disponibilidad mockeada
        // En un escenario real, esto podría consultar otro servicio o base de datos
        return Flux.fromIterable(skus)
                .map(sku -> new Availability(sku, 10)); // Disponibilidad fija de 10 para cada SKU
    }
}
