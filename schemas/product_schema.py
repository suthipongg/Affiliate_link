def product_serializer(product) -> dict:
    product['_source']['id'] = product['_id']
    return product['_source']

def products_serializer(products) -> list:
    return list(map(product_serializer, products))