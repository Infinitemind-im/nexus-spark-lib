# Golden Record Synthesis Spec Guide

Este documento explica, en lenguaje operativo, lo que exigen las secciones enlazadas desde el HTML autoritativo:

- `SparkTransform §5` -> `nexus_spark_lib.transform.synthesise`
- `DataPaths §1.7` -> `Spark Transformer - Stage 3: Golden Record Synthesis`

No reemplaza la spec original. Sirve como lectura de implementacion para entender exactamente que debe hacer `nexus_spark_lib.transform.synthesise` y que espera el pipeline alrededor de ella.

## Fuentes autoritativas

- `mindy-enterprise-docs/docs/developement/specifications/mvp/iteration 2/NEXUS-Iter2-CDM-to-Stores-Diagram.html`
- `mindy-enterprise-docs/docs/developement/specifications/mvp/iteration 2/specs/libraries/NEXUS-Iter2-SPEC-LIB-SparkTransform-v0.1.md`
- `mindy-enterprise-docs/docs/developement/specifications/mvp/iteration 2/specs/pipeline/NEXUS-Iter2-REF-DataPaths-v0.3.md`

## Resumen ejecutivo

La spec pide que Stage 3 haga sintesis de Golden Record por atributo, no por fila completa. La unidad de verdad no es un JSON materializado del negocio, sino una tabla de procedencia por atributo que dice que fuente gana cada atributo y por que regla gano.

La libreria `nexus_spark_lib.transform.synthesise` debe:

1. Ejecutarse solo para records `hot` y no provisionales.
2. Leer las reglas de survivorship por `(tenant_id, cdm_entity_type, attribute_name)`.
3. Comparar el record entrante contra el ganador actual de cada atributo.
4. Insertar o actualizar solo las filas de procedencia que realmente cambian.
5. Recalcular un `provenance_hash` determinista del estado completo del GR.
6. Resolver el caso `DELETE` re-eligiendo ganadores desde el conjunto completo de fuentes supervivientes.
7. No almacenar valores de negocio en la capa canonica; solo punteros y hashes.

## Que pide exactamente SparkTransform §5

## 5.1 Entry point

La entrada formal es:

```python
synthesise(record: NormalisedRecord, resolution: ResolutionResult, ctx: SynthesisContext) -> SynthesisResult
```

La spec impone estas precondiciones:

- Se llama inmediatamente despues de `resolve()`.
- Solo corre para records `hot`.
- `warm` y `cold` no se sintetizan.
- Los records `provisional` tampoco se sintetizan.

Eso significa que `synthesise()` no es una etapa generica para todo record. Es una etapa condicionada por materialization level y por el resultado de ER.

### Flujo obligatorio del entry point

Para cada atributo canonico no nulo que aporta el record:

1. Cargar la procedencia existente del `cdm_entity_id`.
2. Cargar la regla de survivorship de ese atributo.
3. Evaluar si el record entrante gana o no gana ese atributo.
4. Si gana, generar un cambio de procedencia.
5. Persistir ese cambio con una upsert idempotente.
6. Si hubo cambios reales, recomputar `provenance_hash` y actualizar `golden_records_index`.

### Consecuencia funcional

La spec no pide guardar el Golden Record como snapshot de valores. Pide guardar el estado de procedencia por atributo, y derivar desde ahi el estado canonico.

## 5.2 Survivorship evaluation

La decision se toma atributo por atributo. No se resuelve un "winner" global del record completo.

### Reglas soportadas por la spec de libreria

- `source_priority`
- `most_recent`
- `most_complete`
- `most_confident`
- `first_observed`
- `manual`

### Semantica exacta de cada regla

`source_priority`

- Se mira una lista ordenada de conectores.
- Gana el conector con mejor prioridad.
- Las fuentes no listadas quedan al final.

`most_recent`

- Gana la observacion con `source_ts` mas reciente.

`most_complete`

- Gana la fuente con mayor completitud del record.
- La completitud se mide como el conteo de atributos canonicos no nulos.

`most_confident`

- Gana la fuente cuyo match ER tiene mayor confianza.

`first_observed`

- El primer ganador queda congelado.
- Un update posterior no lo reemplaza.

`manual`

- El pipeline no puede sobreescribirlo.
- Si hay override manual, `synthesise()` debe dejar ese atributo intacto.

### Regla clave que suele confundirse

En el camino normal de UPSERT o UPDATE, la evaluacion se hace contra el ganador actual del atributo, no contra todas las fuentes historicas. La reevaluacion completa sobre todo el conjunto de fuentes se reserva para:

- `handle_source_delete()`
- el DAG de `survivorship-rebuild`

## 5.3 Idempotent provenance upsert

Esta parte es una exigencia central de la spec, sobre todo para backfill y replay.

### Lo que debe garantizar

- Primer procesamiento: inserta.
- Replay del mismo evento: no cambia nada.
- Cambio real del valor: actualiza.

### Clave de idempotencia

La fila de procedencia se upsertea por:

- `cdm_entity_id`
- `attribute_name`

Y la actualizacion solo debe ocurrir cuando el `observed_value_hash` cambie.

### Que implica esto

No basta con hacer una upsert ciega. La spec pide una upsert condicionada por hash para que:

- backfill replay sea no-op,
- redelivery de Kafka sea no-op,
- y solo haya escritura cuando el estado semantico del atributo cambie.

## 5.4 Provenance hash computation

La spec de libreria endurece aqui el contrato.

### Hash que pide SparkTransform §5.4

El `provenance_hash` debe ser un SHA-256 determinista del estado completo del Golden Record. La entrada del hash es:

- la lista ordenada por atributo de `(attribute_name, winning_record_id, observed_value_hash)`

Es decir, el hash no depende solo de que record gana, sino tambien del hash del valor observado.

### Por que esto importa

Si gana el mismo `winning_record_id` pero el valor cambió, el hash tambien debe cambiar. Ese detalle es importante para detectar cambios reales de contenido sin releer todas las filas de procedencia.

### Diferencia con DataPaths §1.7

`DataPaths §1.7` resume el hash como SHA-256 del conjunto ordenado de `(attribute_name, winning_record_id)`.

`SparkTransform §5.4` es mas especifico y mas estricto: incluye tambien `value_hash`.

Interpretacion operativa recomendada:

- `DataPaths §1.7` describe la idea funcional a nivel pipeline.
- `SparkTransform §5.4` define el algoritmo exacto de libreria.

Por tanto, para implementar `nexus_spark_lib.transform.synthesise`, la referencia mas fuerte es `SparkTransform §5.4`.

## 5.5 Edge case: Source DELETE - survivorship re-election

Esta es la parte mas dura de la spec.

### Que debe pasar cuando llega un DELETE

1. Buscar el `cdm_entity_id` del source borrado en `entity_resolution_index`.
2. Identificar que atributos estaban siendo ganados por esa fuente.
3. Eliminar la fuente borrada del `entity_resolution_index`.
4. Obtener el conjunto restante de fuentes que aun contribuyen al mismo GR.

Luego hay dos ramas:

### Rama A: no quedan fuentes

- El Golden Record pasa a estado `tombstoned`.
- Se elimina toda la procedencia del GR.
- Se emite un `REMOVE`.

### Rama B: quedan fuentes

Para cada atributo afectado:

1. Quitar la fila de procedencia que apuntaba a la fuente borrada.
2. Releer los valores actuales de las fuentes supervivientes desde Delta Lake.
3. Re-ejecutar la regla de survivorship sobre el conjunto completo de candidatos supervivientes.
4. Si aparece un nuevo ganador, insertar su nueva fila de procedencia.
5. Si ninguna fuente aporta ese atributo, el atributo desaparece del GR.

### Restriccion mas importante del DELETE

La re-eleccion debe correrse siempre contra el conjunto completo de fuentes supervivientes, nunca como una comparacion incremental solo contra el "siguiente posible ganador".

La spec lo impone para garantizar determinismo independiente del orden historico de procesamiento.

### Unica zona donde Stage 3 toca Delta Lake

La propia spec dice que la re-eleccion por DELETE es la unica ruta donde `synthesise()` necesita leer Delta Lake. Esto es importante porque obliga a tener un reader capaz de resolver el valor actual de un atributo desde `(connector_id, source_table, source_record_id, attribute_name)`.

## Que pide DataPaths §1.7

`DataPaths §1.7` describe el mismo Stage 3, pero desde la perspectiva del flujo de datos del pipeline.

### Creacion inicial del GR

Si no existe procedencia previa para ese `cdm_entity_id`:

- el record es el unico contribuyente,
- cada atributo canonico no nulo genera una fila en `golden_record_provenance`,
- y ese source gana todo por defecto porque todavia no hay competidores.

### Cuando una segunda fuente cae en el mismo GR

La spec de DataPaths obliga a correr survivorship por atributo y permite tres efectos:

- `UPDATE` de la procedencia si la nueva fuente gana,
- sin cambios si sigue ganando la fuente actual,
- `INSERT` de una nueva fila si la nueva fuente aporta un atributo que antes nadie aportaba.

### En UPDATE de un source record

No todo update debe rehacer todo.

La spec pide:

1. Calcular diff de atributos entre `before_payload` y `after_payload`.
2. Si cambio algun atributo ER-relevante, rehacer ER primero.
3. Rehacer survivorship solo para los atributos cambiados.

Eso evita recalcular innecesariamente atributos que no cambiaron.

### En DELETE

`DataPaths §1.7` coincide con `SparkTransform §5.5` en lo esencial:

- quitar filas de procedencia del source borrado,
- re-elegir por atributo desde las fuentes restantes,
- usar Delta Lake para leer valores supervivientes,
- tombstone si ya no queda procedencia,
- emitir `operation='REMOVE'` si el GR desaparece.

### Hash segun DataPaths

El texto de DataPaths lo resume como hash del conjunto ordenado de `(attribute_name, winning_record_id)`. A nivel de implementacion precisa, esto queda refinado por `SparkTransform §5.4`.

### Tablas que Stage 3 debe escribir

Segun `DataPaths §1.7`, Stage 3 escribe:

- `nexus_system.golden_record_provenance`
- `nexus_system.golden_records_index`

Y no debe escribir valores de negocio; solo procedencia, identidad y hash.

## Que sale de Stage 3 y para que sirve

La sintesis no es un final aislado. Alimenta el evento `m1.int.transformed_records`.

La salida que espera el pipeline incluye:

- `cdm_entity_id`
- operacion (`UPSERT`, `REMOVE`, `RELEVEL` segun el caso)
- `fields` con `value` o `value_ref`
- `is_winner` por atributo
- `provenance_summary`
- `provenance_hash`

El objetivo es que el resto del pipeline sepa:

- que atributos gano cada fuente,
- si el contenido canonico cambio,
- y si necesita reproyectar a stores downstream.

## Restricciones no negociables que salen de ambas specs

### 1. Virtual CDM

No se almacenan valores de negocio en la capa canonica de procedencia. Solo:

- identificadores de fuente,
- record ids,
- timestamps,
- hashes,
- reglas aplicadas.

### 2. Determinismo

Con el mismo conjunto de fuentes y la misma configuracion de reglas, el resultado debe ser siempre el mismo.

### 3. Idempotencia

Reprocesar el mismo evento no puede crear cambios netos.

### 4. Sintesis por atributo

La competencia es por atributo, no por record completo.

### 5. DELETE fuerte

El caso DELETE no se resuelve quitando solo la fuente borrada. Hay que re-elegir para cada atributo afectado sobre todas las fuentes supervivientes.

## Donde DataPaths simplifica y SparkTransform precisa

Hay varias simplificaciones del texto de pipeline respecto al texto de libreria:

### Hash

- DataPaths: habla de `(attribute_name, winning_record_id)`.
- SparkTransform: precisa `(attribute_name, winning_record_id, value_hash)`.

### Reglas

- DataPaths enumera los tipos de regla desde la vista de negocio.
- SparkTransform define la logica exacta de comparacion y tie-break por codigo.

### DELETE

- DataPaths lo describe funcionalmente.
- SparkTransform define el algoritmo detallado, incluyendo lookup inicial, borrado del ER index, re-election y tombstone.

## Checklist de implementacion para `nexus_spark_lib.transform.synthesise`

Si el modulo quiere cumplir la spec completa, debe satisfacer este checklist:

- Saltar records `warm`, `cold` y `provisional`.
- Cargar procedencia completa del GR al entrar.
- Cargar reglas de survivorship por tenant y entity type.
- Evaluar atributo por atributo solo para valores no nulos.
- Respetar `manual` y `first_observed`.
- Hacer upsert idempotente por atributo con guardia de hash.
- Recalcular `provenance_hash` solo cuando haya cambio real o GR nuevo.
- Actualizar `golden_records_index.provenance_hash`.
- En `DELETE`, borrar la fuente del ER index y re-elegir sobre todas las fuentes supervivientes.
- Leer valores supervivientes desde Delta Lake en la ruta de DELETE.
- Tombstonar el GR cuando no quede procedencia.
- Emitir la operacion correcta downstream (`UPSERT` o `REMOVE`).

## Lectura final

La spec no define Stage 3 como "guardar el mejor record". Define Stage 3 como un motor determinista de procedencia por atributo, con hash de estado e idempotencia fuerte, y con un caso DELETE que obliga a reevaluacion completa del survivor set.

Si una implementacion hace solo comparacion fila contra fila, no guarda procedencia por atributo, o maneja DELETE sin re-election completo desde Delta Lake, entonces no cumple la spec completa enlazada desde el HTML.