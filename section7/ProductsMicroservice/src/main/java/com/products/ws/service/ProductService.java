package com.products.ws.service;

import com.products.ws.rest.CreateProductRestModel;

public interface ProductService {

    String createProduct(CreateProductRestModel productRestModel) throws Exception;

}
