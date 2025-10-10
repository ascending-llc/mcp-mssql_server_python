SELECT v.name   AS ViewName,
       c.name   AS ColumnName,
       ep.value AS Description
FROM sys.views v
         INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
         LEFT JOIN sys.columns c ON c.object_id = v.object_id
         LEFT JOIN sys.extended_properties ep
                   ON ep.major_id = v.object_id
                       AND ep.name = c.name
                       AND ep.minor_id = 0
                       AND ep.class = 1
WHERE s.name = 'AI'
  AND v.name = 'v_SL_Reviews'
ORDER BY c.column_id;