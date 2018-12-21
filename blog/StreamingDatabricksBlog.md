# Streaming Machine Learning Solutions With Azure Databricks

Organizations are beginning not only benefit from streaming data solutions, but require them to differentiate themselves from their competitors.  Real-time reporting, alerts, and predictions are a common asks for business of all sizes, however, they rarely understand the requirements or implementation details to achieve that level of data processing.  **Streaming data** is information that is generated and consumed continuously. Streaming data typically includes many data sources, and a wide variety of sources such as log files, point of sale data (in store and online), financial data, and IoT Devices. 

## Implementation
### Fast and Easy
Generating business changing insights from streaming data can be a difficult process, however, there are quick wins for organizations of all sizes. 

It is extremely easy to set up data pipelines to extract data from your social media pages, analyze them for sentiment analysis, and alert users when comments or posts need to be address. While this may not be a business changing solution, it gives companies the ability to have a more intimate level of interaction with customers or users that they did not have before. 

Microsoft has easily provided a solution for companies to take advantage of this capability. Simply using Azure Logic Apps and Microsoft Cognitive Services, one can be alerted of any positive or negative tweet that occurs about their company. This is a simple and cost effective way to implement intelligence into a workflows. Check out the example available [here](https://blogs.msdn.microsoft.com/deeperinsights/2017/07/12/how-to-measure-twitter-sentiment-with-azure-logic-apps-sql-database-and-power-bi/). Azure Logic Apps can connect to all kinds of data sources and streams that allow organizations with to implementing real-time reporting using a drag and drop interface.  

### Ideal Implementation    
From my experience, companies benefit most from custom machine learning solutions that solve a specific business problem using their own data. Creating solutions tailored to solve a problem in a specific environment allows a business to truly begin the proactive approach of incorporating intelligence throughout their organization. However, lack knowledge is often a barrier for organizations when implementing custom and scalable solutions. 

[Azure Databricks](https://azure.microsoft.com/en-us/services/databricks/) is an optimized platform for [Apache Spark](https://spark.apache.org/), and enables organizations to easily implement streaming data and predictive analytics (machine learning and deep learning) solutions. It is common for organizations to implement batch processes in Azure Databricks to save on cloud consumption costs, while knowing that they are planning for the future. Azure Databricks is a managed Apache Spark offering that works great for batch and streaming processes, meaning that you can save money initially by turning off your virtual machines but when real-time insights is needed one can simply flip a switch for streaming data. Deploy cost effective infrastructure now with the ability to scale limitlessly as you need in the future. 

Below is a common infrastructure diagram I implement with my customers.  
1. They have a number of data sources (devices, applications, databases etc.) that publish information to an Azure Event Hub (or [Apache Kafka](https://kafka.apache.org/)). 
    - Please note that whatever the data source is, there will always need to be some sort of process or application that collects data and sends it to the Event Hub. For example, when working with IoT Devices I will still need to deploy code to the device that sends data to the Event Hub, which is similar with applications since instead of sending data to a database or log files to cold storage you will publish them to an Event Hub so that you can do stream processing.  
1. Process the stream of data as quickly as possible an untouched to "raw" data storage in an Azure Data Lake Store or Azure Blob Storage. 
1. Cleanse data as needed and stream appropriately to an application database, Power BI, or use [Databricks Delta](https://docs.databricks.com/delta/index.html) for real-time insights, consumption, and intelligent automated actions. Please note that applications can read directly off an Event Hub as a consumer as well.     
1. The use Azure Databricks train a machine learning or deep learning model that can be used to make streaming predictions, or batch data processing for additional analytics. 

![](imgs/Streaming%20Data.png)

#### Tips to Actually Implement a Solution
With all analytic solutions you need to:
1. Identify a business problem to solve and the stakeholders
1. Visualize or surface results to "wow" stakeholders
1. Start developing iteratively

##### Business Problem
Often companies simply start creating a solution to work with newer technology without a true business problem they are trying to solve. This is extremely common with organizations who want to implement a data lake strategy. Their main goal is to develop a data lake so that other business units can take advanatage of the sandbox environment for predictive analytics. This is a great idea for an IT and Business organization, however, without a business problem they are trying to solve the organization won't see adoption of this new solution. 

##### Wow Stakeholders
There is not a more boring outcome to a business stakeholder than a project resulting in code. There has to be some type of end product that can make accurately describe how awesome the solution created is. In most machine learning solutions that I implement I will almost always provide a Power BI Report so that the model and predictions are tangible and leave a business user with the ability to actually use the predictions. 


##### Iteractive Development
The most frustrating part of projects can be the initial planning or analysis phase. Large enterprises will often start a project and get stuck in analysis paralysis. I encourage teams I work with to simply start coding! This does not mean to do zero planning or proof of concepts, but at some point a team has to pick a direction and run with it. 

## Benefits
Streaming data architecture is beneficial in most scenarios where dynamic data is generated on a continual basis. Any industry can benefit from data being available almost instantly from the time it was created. Most organizations will begin with simple solutions to collect log data, detect outliers based off set (unintelligent) rules, or provide real-time reporting. However, they will evolve to become more sophisticated data processing pipelines that can learn and detect fraudulent charges as they occur. The true advantage of streaming data is to perform advanced tasks, like machine learning, to take preventative or proactive action. 

Processing a data stream is extremely effective at generating quick insights, but it does not replace batch process. Typically organizations will implement both solutions in order to have the best of both worlds. Streaming data to react or anticipate events, and batch processing to derive additional insights after the fact. Batch processing can often require more compute and is ideal for when time or speed is not a priority. The great aspect is with Azure Databricks, companies are able to use the same infrastructure for both their workflows!   

There are several advantages of streaming data solutions over batch processing: 
- Batch processing data requires a system to allow data to build up so that it can be processed all at once. This often requires larger compute resources than streaming due to the size of data, which can be a hurdle for most organizations. Streaming solutions do require less compute but require machines to be running 100% of the time. 
- 

## Example
I recently created a simple walkthrough of how to implement a streaming data solution on Azure. Check out the walkthrough [here](https://github.com/ryanchynoweth44/StreamingExampleDatabricks). Please note that it requires the following:

 - [Azure Subscription](https://azure.microsoft.com/en-us/free/search/?&OCID=AID719825_SEM_KX8R84uR&lnkd=Bing_Azure_Brand&msclkid=6e706d7f2c60158ed7103168c2415255&dclid=CNmloKvCp98CFVJgwQodwMcKKQ)
 - Basic Knowledge of Python and/or Scala

 ## Conclusion
 I strongly believe that organizations of any size can benefit from a streaming solution using Databricks and Azure Data Lake Store. It enables near real-time reporting, as well as, provides a sandbox environment for iterative development of intelligent solutions. 