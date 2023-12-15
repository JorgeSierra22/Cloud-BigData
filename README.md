# Cloud-BigData

## Introducción
Para nuestro trabajo, queremos llevar a cabo un análisis profundo sobre todos los crímenes que han sucedido en la ciudad de Chicago, en Estados Unidos, comenzando en 2001 hasta la actualidad. 

Nuestro objetivo principal es poder sacar conclusiones y patrones, gracias a las herramientas de Big-Data, a cerca de tendencias reconocibles que se han producido en dicha ciudad. En específico, nos queremos centrar en obtener cuales han sido los tipos de crimenes más comunes, la franja horaria más conflictiva(en la que se han producido más delitos), ... . Con estos resultados, nuestra meta es poder ayudar a la policía de Chicago a mejorar la seguridad de una de las ciudades más pobladas de todo Estados Unidos(Actualmente la 3ª más poblada de todo EEUU según https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population) y además extrapolar estos resultados para un uso global. 

## Datos
La identificación y adquisición de los datos para nuestro proyecto se llevó a cabo mediante una búsqueda exhaustiva entre diversas fuentes de datos proporcionadas en el material de referencia. Tras una evaluación de varias opciones, se identificó que data.gov era la plataforma que albergaba conjuntos de datos relevantes, particularmente relacionados con Estados Unidos, lo que nos permitió acceder a la información necesaria para llevar a cabo nuestra investigación.

Cabe destacar que los datos que utilizamos provienen directamente del Chicago Police Department's CLEAR (Citizen Law Enforcement Analysis and Reporting) system, lo que asegura su origen oficial y su confiabilidad en el contexto del estudio de crímenes en la ciudad de Chicago.

La estructura de los datos se compone de múltiples campos, que incluyen información crucial para nuestro análisis. Estos **campos** son los siguientes: *id, case number, date, block, IUCR, Primary type, Description, Location Description, Arrest, Domestic, Beat, District, Ward, Community Area, FBI code, X-coordinate, Y-coordinate, Year, Update-On, Latitude, Longitude y Location*.

En cuanto al tamaño de los datos, estos ocupan un total de 1.74GB, lo que refleja la magnitud del conjunto de datos y justifica aún más la elección de técnicas de Big Data processing para su procesamiento y análisis. Su formato es .cvs como en los Assignments.

Este enfoque meticuloso en la identificación y selección de datos confiables y representativos sienta las bases para una investigación sólida y detallada sobre el índice de criminalidad en Chicago, permitiéndonos abordar de manera efectiva la detección de patrones y tendencias delictivas en la ciudad.

En nuestro Github se pueden encontrar en la carpeta [datsets](https://github.com/JorgeSierra22/Cloud-BigData/tree/main/datasets)


## Código
Para nuestro trabajo, hemos decidido realizar script de Spark en el lenguaje de python. Todos los códigos se encuentran en la carpeta [code](https://github.com/JorgeSierra22/Cloud-BigData/tree/main/code)

### Cómo ejecutar los códigos
Para poder ejecutar nuestros códigos en **local** se deben de realizar los siguientes pasos:

**Instalar el entorno**:
Primero nos debemos de asegurar que tenemos un sistema operativo que soporte Unix, ya sea el propio Linux, wsl en Windows, o  Mac OS. 

Una vez nos encontremos en la terminal de dicho sistema operativo:

1º Instalar java y pip:
```
sudo apt install default-jre pip
``` 
2º Instalar PySpark:
```
pip install pyspark
``` 
3º Actualizar la ruta: 
```
source ~/.profile
``` 

Siguiendo estos pasos, conseguimos poder ejecutar Spark en modo local


**Ejecutar nuestros Scripts**:

1º Debemos de descargar el dataset. Para ello, nos vamos a la carpeta [datsets](https://github.com/JorgeSierra22/Cloud-BigData/tree/main/datasets) y seguimos los pasos de información importante.

2º Debemos descargar la carpeta [code](https://github.com/JorgeSierra22/Cloud-BigData/tree/main/code). En ella encontramos diferentes archivos de python. 

3º Asegurándonos que tenemos el código y el dataset en la misma carpeta y además nosotros nos encontramos en dicha ruta, poniendo el siguiente comando en la terminal ```spark-submit example.py```, se procesa el script deseado.

4º Para ver los resultados se habrá creado una carpeta nueva en dicha ruta que se podrá visualizar poniendo el siguiente comando: ```more ouput_example/*```. Sin embargo, todos estos resultados se encuentran visibles en la carpeta results

### Explicación código
-[hours.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/hours.py) : A través de este programa realizamos una agrupación de los delitos cometidos por cada franja horaria durante todo el tiempo del estudio.

-[listOfStreets.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/listOfStreet.py) : Gracias a este programa podemos ver las calles que han sido atendidas por cada comisaría durante el periodo de tiempo del estudio.

-[most_problematic_street_for_district.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/most_problematic_street_for_district.py): Este código es añade información extra al script anterior [listOfStreets.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/listOfStreet.py) y nos muestra la calle que más ha sido atendida por cada comisaría en todos los años del estudio.

-[typeOfCrime.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/typeOfCrime.py) : Con este código, vemos cuántos crímenes de cada tipo se han producido en los años del estudio.

-[typeOfCrime5.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/typeOfCrimes5.py): Se trata de una profundización del codigo [typeOfCrime.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/typeOfCrime.py) en donde en vez de mostrar el tipo de crimen asociado a su año, estos salen agrupados en franjas de 5 años((2001, 2006), (2007, 2012), (2013, 2018), (2019, 2023)) para poder ver mejor la evolución de las tendencias criminales en Chicago.

-[crimesEachYearOnDistrict.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/crimesEachYearOnDistrict.py): Llevamos a cabo la cuenta del número de delitos que han sido atendidos por cada comisaría en cada año.

-[maxCrimeEachYearOnADistrict.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/maxCrimeEachYearOnADistrict.py): Este script nos enseña el máximo número de delitos realizados en cada año del estudio y se complementa con el código [crimesEachYearOnDistrict.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/crimesEachYearOnDistrict.py).

-[crimesWithArrest](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/crimesWithArrest.py): Por medio de este script, podemos ver los crímenes en los cuales la policía de Chicago arrestó al delincuente, este código está relacionado con el script de [typeOfCrime.py](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/typeOfCrime.py) que ya hemos explicado anteriormente.

-[grep_streets](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/code/grep_streets.py): Para finalizar, este script simula el comando grep para encontrar las calles en las que se cometieron delitos en algún momento dado un año y un tipo de crimen. Es importante recalcar que para este script se debe de ejecutar este comando ```spark-submit grep_streets.py THEFT 2023```.


### Resultados
Para visualizar los resultados que hemos obtenido podemos ir a la carpeta [results](https://github.com/JorgeSierra22/Cloud-BigData/tree/main/results) donde se encuentran los csv resultantes de ejecutar los códigos explicados anteriormente y, en especial, para saber las conclusiones a las que hemos llegado se puede consultar tanto nuesta [página web](https://github.com/JorgeSierra22/Cloud-BigData/tree/main/webPage) como el archivo de Jupyter notebook [results.ipybn](https://github.com/JorgeSierra22/Cloud-BigData/blob/main/results/results.ipynb) en el cual, a través de la manipulación de las librerías de python Pandas y MatPlotLib, se encuentran las gráficas usadas en la web.
