def affiliate_link_serializer(aff) -> dict:
    aff['_source']['id'] = aff.pop('_id')
    return aff['_source']

def affiliate_links_serializer(affs) -> list:
    return list(map(affiliate_link_serializer, affs))