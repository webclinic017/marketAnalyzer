

def delete_fields_modeL(data):
    for e in ['changed_by', 'changed_by_name', 'changed_by_url', 'changed_on_delta_humanized',
              'changed_on_utc', 'created_by', 'edit_url', 'id', 'last_saved_at', 'last_saved_by',
              'url', 'thumbnail_url', "table", "datasource_url", "datasource_name_text", "description_markeddown",
              "owners"]:
        if e in data.keys():
            data.pop(e)
    return data
