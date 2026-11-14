# Azure Storage

La PoC utiliza una misma cuenta Storage y tres contenedores privados:

- `openshift-ha`
- `flink-checkpoints`
- `flink-savepoints`

La cuenta debe tener habilitado el endpoint DFS/ADLS Gen2 para las rutas `abfs://...dfs.core.windows.net`.

El Secret `azure-secret-st-flinkdata` debe existir en el namespace del job y contener:
- `azurestorageaccountname`
- `azurestorageaccountkey`

El chart utiliza `kubernetes.env.secretKeyRef` para inyectar las claves en los pods Flink y `FLINK_PROPERTIES` para que el entrypoint expanda las variables al preparar `config.yaml`. La clave no se guarda en ConfigMaps ni en Git.
