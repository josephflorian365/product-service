package com.splitcart.product.repo;

import com.splitcart.product.model.Product;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

/*
Completar el repository que consuma de mongoDb de manera reactiva los productos
 */
@Repository
public interface ProductRepository extends ReactiveMongoRepository<Product, String> {

    // Método para buscar productos por categoría
    Flux<Product> findByCategory(String category);

    // Método para buscar productos por precio máximo
    Flux<Product> findByPriceLessThanEqual(Double maxPrice);

    // Método para buscar productos por categoría y precio máximo
    Flux<Product> findByCategoryAndPriceLessThanEqual(String category, Double maxPrice);
}
