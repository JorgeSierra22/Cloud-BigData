# Cloud-BigData

## Introducción
Para nuestro trabajo, queremos llevar a cabo un análisis profundo sobre todos los crímenes que han sucedido en la ciudad de Chicago, en Estados Unidos, comenzando en 2001 hasta la actualidad. Nuestro objetivo principal es poder sacar conclusiones y patrones, gracias a las herramientas de Big-Data, a cerca de tendencias reconocibles que se han producido en dicha ciudad. En específico, nos queremos centrar en obtener cuales han sido los tipos de crimenes más comunes, la franja horaria más conflictiva(en la que se han producido más delitos), ... . Con estos resultados, nuestra meta es poder ayudar a la policía de Chicago a mejorar la seguridad de una de las ciudades más pobladas de todo Estados Unidos(Actualmente la 3ª más poblada de todo EEUU según https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population) y además extrapolar estos resultados para un uso global. 

## Datos
La identificación y adquisición de los datos para nuestro proyecto se llevó a cabo mediante una búsqueda exhaustiva entre diversas fuentes de datos proporcionadas en el material de referencia. Tras una evaluación de varias opciones, se identificó que data.gov era la plataforma que albergaba conjuntos de datos relevantes, particularmente relacionados con Estados Unidos, lo que nos permitió acceder a la información necesaria para llevar a cabo nuestra investigación.

Cabe destacar que los datos que utilizamos provienen directamente del Chicago Police Department's CLEAR (Citizen Law Enforcement Analysis and Reporting) system, lo que asegura su origen oficial y su confiabilidad en el contexto del estudio de crímenes en la ciudad de Chicago.

La estructura de los datos se compone de múltiples campos, que incluyen información crucial para nuestro análisis. Estos **campos** son los siguientes: *id, case number, date, block, IUCR, Primary type, Description, Location Description, Arrest, Domestic, Beat, District, Ward, Community Area, FBI code, X-coordinate, Y-coordinate, Year, Update-On, Latitude, Longitude y Location*.

En cuanto al tamaño de los datos, estos ocupan un total de 1.74GB, lo que refleja la magnitud del conjunto de datos y justifica aún más la elección de técnicas de Big Data processing para su procesamiento y análisis. Su formato es .cvs como en los Assignments.

Este enfoque meticuloso en la identificación y selección de datos confiables y representativos sienta las bases para una investigación sólida y detallada sobre el índice de criminalidad en Chicago, permitiéndonos abordar de manera efectiva la detección de patrones y tendencias delictivas en la ciudad.

En nuestro Github se pueden encontrar en la carpeta **datsets**: https://github.com/JorgeSierra22/Cloud-BigData/tree/main/datasets

## Código
Para nuestro trabajo, hemos decidido realizar script de Spark en el lenguaje de python. Todos los códigos se encuentran en la carpeta **code**: https://github.com/JorgeSierra22/Cloud-BigData/tree/main/code

### Cómo ejecutar los códigos

