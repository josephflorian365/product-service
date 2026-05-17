package com.splitcart.product.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
Agregar las anotaciones de lombok para getters, setters y constructores
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Availability {

  private String sku;
  private int available;
}
