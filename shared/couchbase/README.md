# Couchbase para el laboratorio

El sink Couchbase es opcional. El valor por defecto de la PoC es `couchbase.sinkMode: console`.

Cuando exista un Couchbase accesible desde los pods:
1. Configura `connectionString`, bucket, scope y collection.
2. Crea el Secret `couchbase-credentials` en el namespace del job con claves `username` y `password`.
3. Cambia `couchbase.sinkMode` a `couchbase`.
4. Sincroniza/reinicia el FlinkDeployment.

No uses `localhost` desde un pod OpenShift. Debe ser un Service DNS o endpoint alcanzable desde el namespace.

El docker-compose es solo para Couchbase local; no hace que OpenShift pueda acceder automáticamente al contenedor Docker de tu PC.
