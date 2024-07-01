def parse_sort_criteria(sort_by: str, reverse=False):
    """
    Parse the sort_by string into a list of sorting criteria.

    Example:
    Input: 'field1,-field2'
    Output: [('field1', ASCENDING), ('field2', DESCENDING)]
    """
    desc = "desc" if reverse else "asc"
    asc = "asc" if reverse else "desc"
    return [
        {sort.strip()[1:]: {"order": desc}}
        if sort.strip().startswith("-")
        else {sort.strip(): {"order": asc}}
        for sort in sort_by.split(",")
    ]

def type_query(field, value):
        field_type = field.split('.')
        if len(field_type) == 2:
            f, type_q = field_type
            if type_q == 'range':
                return {"range": {f: value}}
        if isinstance(value, list):
            return {"terms": {field: value}}
        return {"term": {field: value}}

def parse_filter_criteria(filter_body: str):
    """
    Parse the filter query string into a dictionary of filter criteria.

    Example:
    Input: 'field1:value1,field2:*value2*'
    Output: {'field1': 'value1', 'field2': {'$regex': '.*value2.*'}}
    """

    filter_criteria = [
        type_query(key, val)
        for key, val in filter_body.items()
    ]
    
    return {
        "bool": {
            "must": filter_criteria
        }
    }