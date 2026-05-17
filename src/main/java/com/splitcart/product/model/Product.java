package com.splitcart.product.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


/*
Agregar las anotaciones necesarias para que esta clase sea un documento de MongoDB, uso de lombok para getters, setters y constructor sin argumentos
 */
@Document(collection = "products")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {

  @Id private String id;
  private String name;
  private String category;
  private Double price;
}
