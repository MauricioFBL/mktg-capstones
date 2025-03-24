# Presentación: Análisis de Datos de Campañas de Marketing Digital en Plataformas Clave

## 1. Introducción

* Bienvenida al equipo.
* Importancia del análisis de datos en marketing digital:
    * Permite tomar decisiones informadas.
    * Optimiza el rendimiento de las campañas.
    * Mejora el retorno de la inversión (ROI).
* Objetivo de la presentación: comprender las métricas y KPIs clave en las principales plataformas.

## 2. Jerarquía de Campañas y Nomenclatura Estándar

* Estructura Jerárquica:
    * Campaña: Nivel superior, define el objetivo general y el presupuesto.
    * Grupo de Anuncios: Segmenta la audiencia dentro de la campaña.
    * Anuncio: El contenido creativo que se muestra a la audiencia.
* Nomenclatura Uniforme:
    * Utilizar guiones bajos (\_) para separar los elementos de la nomenclatura.
    * Campaña: `IDcampaña_TipoCampaña_Marca`.
        * Ejemplo: `C12345_Awareness_MarcaX`.
    * Grupo de Anuncios: `NombreGrupo_AudienciaObjetivo`.
        * Ejemplo: `InteresadosDeportes_Hombres25a35`.
    * Anuncio: `NombreAnuncio_TipoAnuncio`.
        * Ejemplo: `VideoPromocional_Video`.

## 3. Definiciones Clave

* **Métricas:**
    * Definición: Valores cuantitativos que miden el rendimiento de las campañas de marketing. Responden a la pregunta "¿Cuánto?".
    * **Impresiones:** Número de veces que se muestra un anuncio.
    * **Clics:** Número de veces que se hace clic en un anuncio.
    * **CTR (Click-Through Rate):** Porcentaje de impresiones que resultan en clics. Mide la relevancia del anuncio.
    * **Alcance:** Número de personas únicas que ven un anuncio.
    * **Frecuencia:** Número promedio de veces que una persona ve un anuncio.
    * **Conversiones:** Acciones deseadas realizadas por los usuarios (compras, registros, etc.).
    * **CPA (Costo por Adquisición):** Costo por cada conversión. Mide la eficiencia del gasto publicitario.
    * **CPC (Costo por Clic):** Costo por cada clic. Mide el costo de generar tráfico.
    * **CPM (Costo por Mil Impresiones):** Costo por cada mil impresiones. Mide el costo de llegar a una audiencia.
    * **ROAS (Return on Ad Spend):** Retorno de la inversión publicitaria. Mide la rentabilidad de la campaña.
    * **Vistas:** Número de veces que un usuario ve un anuncio de video.
    * **Cuartiles de Video:** Puntos de control que dividen la reproducción de un video en cuatro segmentos iguales. Miden la retención de la audiencia.
    * **Tasa de Rebote:** Porcentaje de usuarios que abandonan una página después de ver solo una página. Mide la relevancia del contenido.
    * **Tiempo en la Página:** Duración promedio de la visita de un usuario en una página. Mide el compromiso del usuario.
    * **Tasa de conversión:** porcentaje de usuarios que realizan una acción deseada, sobre el total de usuarios que han hecho click en un anuncio. Mide la efectividad del anuncio para generar conversiones.
* **Dimensiones:**
    * Definición: Atributos cualitativos que describen los datos de las campañas. Responden a la pregunta "¿Qué?" o "¿Dónde?".
    * Ejemplos: Plataforma, campaña, audiencia, ubicación, etc.
* **KPIs (Indicadores Clave de Rendimiento):**
    * Definición: Métricas críticas que indican el progreso hacia los objetivos de marketing. Responden a la pregunta "¿Qué tan bien?".
    * Los KPIs son métricas que están directamente relacionadas con los objetivos de negocio.
    * Ejemplos: Tasa de conversión, ROI, etc.

## 4. Desglose por Plataforma

* **Facebook Ads:**
    * Jerarquía: Campaña > Conjunto de Anuncios > Anuncio.
    * Métricas: Impresiones, Clics, CTR, Alcance, Frecuencia, Conversiones, CPA, CPC, ROAS, Vistas de video, Cuartiles de video.
    * KPIs: Tasa de Conversión, ROI.
    * Cuartiles de Video:
        * 25%: Indica el porcentaje de usuarios que vieron al menos el primer cuarto del video. Un bajo porcentaje aquí sugiere que el inicio del video puede no ser atractivo.
        * 50%: Muestra la retención de la audiencia a la mitad del video. Una caída significativa aquí puede indicar que el contenido pierde interés.
        * 75%: Revela cuántos usuarios están cerca de completar el video. Una baja retención aquí puede señalar problemas con la segunda mitad del video.
        * 100%: Indica el porcentaje de usuarios que vieron el video completo. Es el indicador final de la efectividad del video.
* **Google Ads:**
    * Jerarquía: Campaña > Grupo de Anuncios > Anuncio.
    * Métricas: Impresiones, Clics, CTR, Conversiones, CPA, CPC, ROAS, Puntuación de Calidad, Vistas de video (YouTube), Cuartiles de video (YouTube).
    * KPIs: Puntuación de Calidad, ROI.
    * Cuartiles de Video(Youtube): igual que facebook.
* **DV360 (Display & Video 360):**
    * Jerarquía: Campaña > Orden de Inserción > Línea de Pedido > Anuncio.
    * Métricas: Impresiones, Clics, Vistas, Tasa de Finalización de Video, CPA, CPM, ROAS, Visibilidad, Cuartiles de video.
    * KPIs: Visibilidad, Porcentaje de reproduccion de video.
    * Cuartiles de Video: igual que facebook.
* **Teads:**
    * Jerarquía: Campaña > Grupo de anuncios > Creatividad.
    * Métricas: Impresiones visibles, Vistas de video, Tasa de finalización de video, Clics, CTR, Tiempo de visualización.
    * KPIs: VTR (Video Through Rate), Porcentaje de visualización completa, Participación del usuario.
    * Cuartiles de Video: igual que facebook.
* **Bing Ads:**
    * Jerarquía: Campaña > Grupo de Anuncios > Anuncio.
    * Métricas: Impresiones, Clics, CTR, Conversiones, CPA, CPC, ROAS, Cuota de impresiones.
    * KPIs: Cuota de impresiones, Tasa de conversión.
* **Spotify Ads:**
    * Jerarquía: Campaña > Grupo de Anuncios > Anuncio de audio o video.
    * Métricas: Impresiones, Alcance, Frecuencia, Tasa de escucha completa, Clics (display), porcentaje de video visto.
    * KPIs: Reconocimiento de marca, Aumento en el recuerdo del anuncio.
* **Snapchat Ads:**
    * Jerarquía: Campaña > Conjunto de Anuncios > Anuncio.
    * Métricas: Impresiones, Alcance, Vistas de video, Tasa de deslizamiento hacia arriba, Conversiones, Cuartiles de video.
    * KPIs: Participación del usuario, Tasa de conversión.
    * Cuartiles de Video: igual que facebook.
* **TikTok Ads:**
    * Jerarquía: Campaña > Grupo de Anuncios > Anuncio.
    * Métricas: Impresiones, Alcance, Vistas de video, Tasa de interacción, Conversiones, Cuartiles de video.
    * KPIs: Tasa de participación, Tasa de conversión.
    * Cuartiles de Video: igual que facebook.
* **Twitter Ads:**
    * Jerarquía: Campaña > Grupo de Anuncios > Tweet promocionado.
    * Métricas: Impresiones, Interacciones, Clics, Conversiones, Tasa de interacción, porcentaje de video visto.
    * KPIs: Participación del usuario, Alcance.

## 5. Dimensiones Clave

* Plataforma, Campaña, Grupo de Anuncios, Anuncio, Usuario, Tiempo.

## 6. Métricas Principales y KPIs Importantes

* (Ver detalles en el desglose por plataforma).

## 7. Valores Target Estándar de la Industria

* CTR, Tasa de Conversión, ROAS, CPA.


# Tipos de Anuncio por Plataforma: Descripciones y Recomendaciones de Uso

## 1. Facebook Ads

* **Imágenes:**
    * Descripción: Anuncios visuales simples con una imagen y texto.
    * Recomendaciones: Utilizar imágenes de alta calidad y relevantes para la audiencia.
* **Videos:**
    * Descripción: Anuncios dinámicos que captan la atención con contenido en movimiento.
    * Recomendaciones: Crear videos cortos y atractivos que cuenten una historia.
* **Carruseles:**
    * Descripción: Anuncios con múltiples imágenes o videos que los usuarios pueden deslizar.
    * Recomendaciones: Mostrar varios productos o contar una historia secuencial.
* **Colecciones:**
    * Descripción: Anuncios que combinan imágenes y videos con un catálogo de productos.
    * Recomendaciones: Ideal para tiendas en línea que desean mostrar su inventario.
* **Experiencia Instantánea:**
    * Descripción: Experiencias inmersivas de pantalla completa que se cargan rápidamente en dispositivos móviles.
    * Recomendaciones: Crear experiencias interactivas y atractivas para dispositivos móviles.
* **Lead Ads:**
    * Descripción: Formularios dentro de facebook, que permiten captar información de los usuarios.
    * Recomendaciones: Ideal para captar clientes potenciales, sin que estos salgan de la plataforma.

## 2. Google Ads

* **Anuncios de Búsqueda:**
    * Descripción: Anuncios de texto que aparecen en los resultados de búsqueda de Google.
    * Recomendaciones: Utilizar palabras clave relevantes y crear textos persuasivos.
* **Anuncios de Display:**
    * Descripción: Anuncios visuales que se muestran en sitios web y aplicaciones de la Red de Display de Google.
    * Recomendaciones: Utilizar imágenes y videos atractivos y segmentar la audiencia correctamente.
* **Anuncios de Video (YouTube):**
    * Descripción: Anuncios de video que se muestran en YouTube.
    * Recomendaciones: Crear videos atractivos y utilizar la segmentación de YouTube para llegar a la audiencia adecuada.
* **Anuncios de Shopping:**
    * Descripción: Anuncios de productos que muestran imágenes, precios y detalles de los productos.
    * Recomendaciones: Ideal para tiendas online que desean promocionar sus productos.

## 3. DV360 (Display & Video 360)

* **Anuncios de Display Programáticos:**
    * Descripción: Anuncios visuales comprados de forma programática en la web abierta.
    * Recomendaciones: Utilizar segmentación avanzada y control de inventario.
* **Anuncios de Video Programáticos:**
    * Descripción: Anuncios de video comprados de forma programática en la web abierta.
    * Recomendaciones: Utilizar formatos de video premium y segmentación avanzada.
* **Anuncios de Audio:**
    * Descripción: Anuncios de audio comprados de forma programática.
    * Recomendaciones: Utilizar mensajes de audio claros y concisos.
* **Anuncios Nativos:**
    * Descripción: Anuncios que se integran con el contenido del sitio web.
    * Recomendaciones: Asegurar que los anuncios sean relevantes para el contenido del sitio web.

## 4. Teads

* **Anuncios de Video Outstream:**
    * Descripción: Anuncios de video que se reproducen fuera de los reproductores de video tradicionales.
    * Recomendaciones: Utilizar formatos de video premium y contenido atractivo.
* **Anuncios de Display Nativos:**
    * Descripción: Anuncios visuales que se integran con el contenido del sitio web.
    * Recomendaciones: Asegurar que los anuncios sean relevantes para el contenido del sitio web.

## 5. Bing Ads

* **Anuncios de Búsqueda:**
    * Descripción: Anuncios de texto que aparecen en los resultados de búsqueda de Bing.
    * Recomendaciones: Utilizar palabras clave relevantes y crear textos persuasivos.
* **Anuncios de Microsoft Audience Network:**
    * Descripción: Anuncios visuales que se muestran en sitios web y aplicaciones de la red de Microsoft.
    * Recomendaciones: Utilizar imágenes y videos atractivos y segmentar la audiencia correctamente.

## 6. Spotify Ads

* **Anuncios de Audio:**
    * Descripción: Anuncios de audio que se reproducen entre canciones o podcasts.
    * Recomendaciones: Utilizar mensajes de audio claros y concisos.
* **Anuncios de Display:**
    * Descripción: Anuncios visuales que se muestran en la aplicación de Spotify.
    * Recomendaciones: Utilizar imágenes atractivas y relevantes para la audiencia.
* **Anuncios de Video:**
    * Descripción: anuncios de video que se muestran a los usuarios de la versión premium.
    * Recomendaciones: videos cortos y concisos.

## 7. Snapchat Ads

* **Snap Ads:**
    * Descripción: Anuncios de video de pantalla completa que aparecen entre las historias de Snapchat.
    * Recomendaciones: Crear videos cortos y atractivos que aprovechen el formato vertical.
* **Filtros de Realidad Aumentada:**
    * Descripción: Filtros interactivos que los usuarios pueden aplicar a sus fotos y videos.
    * Recomendaciones: Crear filtros divertidos y relevantes para la marca.
* **Lentes de Realidad Aumentada:**
    * Descripción: Experiencias de realidad aumentada que los usuarios pueden usar para interactuar con la marca.
    * Recomendaciones: Crear lentes interactivas y atractivas.

## 8. TikTok Ads

* **Anuncios In-Feed:**
    * Descripción: Anuncios de video que aparecen en el feed de los usuarios.
    * Recomendaciones: Crear videos cortos y atractivos que se adapten al estilo de TikTok.
* **Hashtag Challenges:**
    * Descripción: Desafíos patrocinados que animan a los usuarios a crear contenido relacionado con la marca.
    * Recomendaciones: Crear desafíos divertidos y relevantes para la marca.
* **Efectos de Marca:**
    * Descripción: Efectos interactivos que los usuarios pueden aplicar a sus videos.
    * Recomendaciones: Crear efectos creativos y relevantes para la marca.

## 9. Twitter Ads

* **Tweets Promocionados:**
    * Descripción: Tweets que se muestran a una audiencia específica.
    * Recomendaciones: Utilizar hashtags relevantes
    