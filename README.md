[![Release](https://img.shields.io/github/v/release/igorpereirabr1/hermione_databricks)]((https://pypi.org/project/hermione-databricks/))
![Python Version](https://img.shields.io/badge/python-3.6%20|%203.7%20|%203.8-brightgreen.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

```
  _    _                     _                             
 | |  | |                   (_)                            
 | |__| | ___ _ __ _ __ ___  _  ___  _ __   ___            
 |  __  |/ _ \ '__| '_ ` _ \| |/ _ \| '_ \ / _ \           
 | |  | |  __/ |  | | | | | | | (_) | | | |  __/           
 |_|  |_|\___|_|  |_| |_| |_|_|\___/|_| |_|\___|         
          _____        _        _          _      _ 
         |  __ \      | |      | |        (_)    | |       
         | |  | | __ _| |_ __ _| |__  _ __ _  ___| | _____ 
         | |  | |/ _` | __/ _` | '_ \| '__| |/ __| |/ / __|
         | |__| | (_| | || (_| | |_) | |  | | (__|   <\__ \
         |_____/ \__,_|\__\__,_|_.__/|_|  |_|\___|_|\_\___/
          
 ```                                                         



| Source    | Downloads                                                                                                                       | Page                                                 | Installation Command                       |
|-----------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|--------------------------------------------|
| **PyPi**  | [![PyPi Downloads](https://pepy.tech/badge/hermione_databricks)](https://pypi.org/project/hermione_databricks/)                      | [Link](https://pypi.org/project/hermione_databricks/)        | `pip install -U hermione-databricks `                  |



## What is Databricks?
---
Databricks is an Apache Spark-based analytics platform optimized for the Microsoft Azure/AWS cloud services platforms. Designed with the founders of Apache Spark, Databricks is integrated with Azure/AWS to provide one-click setup, streamlined workflows, and an interactive workspace that enables collaboration between data scientists, data engineers, and business analysts.
Databricks comprises the complete open-source Apache Spark cluster technologies and capabilities. Spark in Azure Databricks includes the following components:

**Spark SQL and DataFrames:** Spark SQL is the Spark module for working with structured data. A DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python.

**Streaming:** Real-time data processing and analysis for analytical and interactive applications. Integrates with HDFS, Flume, and Kafka.

**MLlib:** Machine Learning library consisting of common learning algorithms and utilities, including classification, regression, clustering, collaborative filtering, dimensionality reduction, as well as underlying optimization primitives.

**GraphX:** Graphs and graph computation for a broad scope of use cases from cognitive analytics to data exploration.

**Spark Core API:** Includes support for R, SQL, Python, Scala, and Java.

Reference:
 - https://github.com/databricks
 - https://azure.microsoft.com/en-us/services/databricks/
 - https://databricks.com/aws
---
## What is Hermione?

Hermione is the newest open source library that will help Data Scientists on setting up more organized codes, in a quicker and simpler way. Besides, there are some classes in Hermione which assist with daily tasks such as: column normalization and denormalization, data view, text vectoring, etc. Using Hermione, all you need is to execute a method and the rest is up to her, just like magic.

To bring in a little of **A3Data** experience, we work in Data Science teams inside several client companies and it’s undeniable the excellence of notebooks as a data exploration tool. Nevertheless, when it comes to data science products and their context, when the models needs to be consumed, monitored and have periodic maintenance, putting it into production inside a Jupyter Notebook is not the best choice (we are not even mentioning memory and CPU performance yet). And that’s why Hermione comes in! We have been inspired by this brilliant, empowered and awesome witch of The Harry Potter saga to name this framework!

This is also our way of reinforcing our position that women should be taking more leading roles in the technology field. **#CodeLikeAGirl**

Reference:
 - https://github.com/A3Data/hermione

## What is Hermione-Databricks?

Considering these two fantastic tools, we have bring the Hermione magic to the #databricks environment, considering more scalability through the #pyspark and #Scala.

With  #hermione-databricks you will be able to create the entire structure for your ML project using the databricks workspace to structure the notebooks, pipelines and the DBFS(Databricks File System) to handle with large volumns of data and the project artifacts.

When you start a new project with hermione-databricks, automatcly the bellow local/remote project structures will be created:

<html>
<div>
<div><a href="hermione_databricks\databricks_file_text\pre.ipynb"></a>
<table style="width: 800px;">
<tbody>
<tr style="height: 30px;">
<td style="width: 400px; line-height: 30px; height: 30px;">Local</td>
<td style="width: 400px; line-height: 30px; height: 30px;">Remote</td>
</tr>
<tr style="height: 162px;">
<td style="width: 320px; line-height: 8px; height: 162px;">
<p>.Current Dir</p>
<p>📂project_name</p>
<p>┣ 📜 <a href="hermione_databricks\databricks_file_text\README.ipynb">README.ipynb</a></p>
<p>┣ 📜 <a href="hermione_databricks\databricks_file_text\stack_configuration.json">config.json</a></p>
<p>┣ 📂notebooks</p>
<p>┃&nbsp;&nbsp; ┗📜<a href="hermione_databricks\databricks_file_text\exploratory_analysis.ipynb">exploratory_analysis.ipynb</a></p>
<p>┣📂preprocessing</p>
<p>┃&nbsp;&nbsp; ┗📜<a href="hermione_databricks\databricks_file_text\preprocessing.ipynb">preprocessing.ipynb</a></p>
<p>┗📂model</p>
<p>┃&nbsp;&nbsp; ┗📂Workspace</p>
<p>┃&nbsp;&nbsp;&nbsp;┃ &nbsp;&nbsp; ┗📜<a href="hermione_databricks\databricks_file_text\model.ipynb">model.ipynb</a></p>
<p>┃&nbsp;&nbsp; ┗📂dbfs</p>
<p>┃&nbsp;&nbsp;&nbsp;┃&nbsp;&nbsp; ┗📂input</p>
<p>┃&nbsp;&nbsp;&nbsp;┃&nbsp;&nbsp; ┗📂output</p>
<p>┗&nbsp;&nbsp; ┗&nbsp;&nbsp; ┗📂artifacts</p>
</td>
<td style="width: 320px; line-height: 8px; height: 162px;">
<p>.Workspace</p>
<p>📂project_name</p>
<p>┣ 📜 <a href="hermione_databricks\databricks_file_text\README.ipynb">README.ipynb</a></p>
<p>┣ 📂notebooks</p>
<p>┃&nbsp;&nbsp; ┗📜<a href="hermione_databricks\databricks_file_text\exploratory_analysis.ipynb">exploratory_analysis.ipynb</a></p>
<p>┣📂preprocessing</p>
<p>┃&nbsp;&nbsp; ┗📜<a href="hermione_databricks\databricks_file_text\preprocessing.ipynb">preprocessing.ipynb</a></p>
<p>┗📂model</p>
<p>┗&nbsp;&nbsp; ┗📜<a href="hermione_databricks\databricks_file_text\model.ipynb">model.ipynb</a></p>
<p></p>
<p>.dbfs:</p>
<p>📂project_name</p>
<p>┃&nbsp;&nbsp; ┗📂input</p>
<p>┃&nbsp;&nbsp; ┗📂output</p>
<p>┗&nbsp;&nbsp; ┗📂artifacts</p>
</td>
</tr>
</tbody>
</table>
</div>
</div>
</html>

It's important to note that they are not an exact mirror, the reason is the natural difference of local and remote environments,especially considering the DBFS.

After create the project, you can sync the local remote files with the bellow functions:

- ```hermione_databricks sync-local```  Sync local project(folders/notebooks/model.pkl).
- ```hermione_databricks sync-remote``` Sync remote project(folders/notebooks/model.pkl).

Requirements
------------

-  Python Version >= 3.6

Installation
---------------

To install simply run
``pip install --upgrade hermione-databricks``


Then set up authentication using an `authentication token: <https://docs.databricks.com/api/latest/authentication.html#token-management>`_. Credentials are stored at ``~/.databrickscfg``.

- ``hermione_databricks setup`` (enter hostname/auth-token at prompt)

To test that your authentication information is working, try a quick test like ``databricks workspace ls``.


## How do I use hermione-databricks?
After installed hermione-databricks:

0.  Configure the Databricks autentication :

```
hermione_databricks setup
```
Here you need to specify the databricks host and the access token, The integration will be made using the official databricks-cli library.

1. Starting a new databricks project

```
hermione_databricks new
```
Here the hermione-databricks will ask by the:
- **Project Name:** your project name;
- **Project Description:** Quicly project description;
- **Databricks Host Workspace path:** Databricks workspace path, location where your workspace objects will be saved
- **Databricks Host DBFS path:** Databricks DBFS path, location where your DBFS objects will be saved(**include the dbfs:/ prefix**).

![step_by_step](images/hermione_databricks_new_project.png)

After This, you can see the project files localy:

![step_by_step](images/hermione_databricks_new_project_2.png)

Databricks Wokspace (Databricks CLI):

![step_by_step](images/hermione_databricks_new_project_3.png)

Databricks Wokspace (Databricks Web Interface):

![step_by_step](images/hermione_databricks_new_project_4.png)



## Contributing

  Make a pull request with your implementation.

For suggestions, contact us: igor.pereira.br@gmail.com

## Licence
Hermione-Databricks is open source and has Apache 2.0 License: [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)