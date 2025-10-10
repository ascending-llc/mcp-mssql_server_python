SELECT SCHEMA_NAME(v.schema_id) as SchemaName,
       v.name                   as ViewName,
       ep.value                 as Description
FROM sys.views v
         LEFT JOIN sys.extended_properties ep ON ep.major_id = v.object_id
    AND ep.minor_id = 0 AND ep.name = 'MS_Description'
WHERE SCHEMA_NAME(v.schema_id) = 'AI';