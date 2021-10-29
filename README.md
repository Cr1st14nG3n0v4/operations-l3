# operations-l3

## Description

ETL para la generación de tablas de hechos cuyos datos contienen los indicadores de seguimiento de OPS. Principalmente basado en Reviews y a diferente nivel de agrupación (país, branch, brand, todas juntas).-

La escritura se realiza en Postgres. Dentro de la BBDD, las tablas de hechos almacenan la historia de cada ejecución. 

El flujo de ejecución llama, previamente a finalizar, a una función que se encarga de actualizar los datasets de Quicksight (los Tableros actualmente están construidos en Quicksight). Se envía como parámetro el prefijo de tablas que han sido pobladas por este ETL.

### Generación ETL

#### Operations (fct_ops_***)

<ul>
    <li>Reviews por Momento del día (fct_ops_***)</li>
        <ul>
            <li>fct_ops_orders_score_slottime</li>
        </ul>
    </li>
    <li>Reviews con Apertura (Producto/Key Points) (fct_ops_***)</li>
        <ul>
            <li>fct_ops_orders_score_slottime_options</li>
            <li>fct_ops_orders_score_slottime_products</li>
        </ul>
    </li>
</ul>


## Parámetros

El lambda recibe como parámetros un flag indicando si se ejecuta a nivel histórico el procesamiento o no. Actualmente por defecto se encuentra unicamente el proceso sobre la APP de Rappi y la especificación adicional se basa a nivel país.

<ul>
<li><strong>fg_history</strong>: integer <em>(1 para ejecutar historia, 0 caso contrario)</em></li>
<li><strong>country</strong>: XX, dominio de País <em>(AR por defecto)</em></li>
</ul>


## Nomenclatura BBDD

Con el fin de poder ordenar y tener una vista clara de las fuentes de datos que van poblando a la BBDD Postgre, se optó por utilizar una nomenclatura en las tablas que permita su identificación a partir de prefijos.

<ul>
<li><strong>fct_:</strong> facts table (tabla de hechos)</li>
<li><strong>dim_:</strong> dimension table (tabla con maestro)</li>
<li><strong>lk_:</strong> lookup table (tabla con información anexa a feature)</li>
<li><strong>v_:</strong> view (actualmente funcionando como una report table)</li>
</ul>

<em>Todas las tablas son seguidas a continuación por otro prefijo que indica el Tablero que está consumiendo a las mismas (ver generación de métricas de ETL).</em>