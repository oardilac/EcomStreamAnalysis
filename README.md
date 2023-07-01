<a name="readme-top"></a>
[![Contributors](https://shields.io/badge/Contributors-1-green)](https://github.com/oardilac/EcomStreamAnalysis/graphs/contributors)
[![Forks](https://img.shields.io/github/forks/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/EcomStreamAnalysis/network/members)
[![Stargazers](https://img.shields.io/github/stars/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/EcomStreamAnalysis/stargazers)
[![Issues](https://img.shields.io/github/issues/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/EcomStreamAnalysis/issues)
[![MIT License](https://img.shields.io/github/license/oardilac/Conversational-AI-Chatbot)](https://github.com/oardilac/EcomStreamAnalysis/blob/main/LICENSE)
[![LinkedIn](https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555)](https://www.linkedin.com/in/oardilac/)
<br />
<div align="center">
    <h3 align="center">EcomStreamAnalysis: Spark and Kafka</h3>

   <p align="center">
    This project illustrates the use of Apache Spark, Kafka and Cassandra with Docker to perform data ingestion and data processing.    
    <br />
  <p align="center">
    <a href="https://github.com/oardilac/EcomStreamAnalysis/"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/oardilac/EcomStreamAnalysis/">View Demo</a>
    ·
    <a href="https://github.com/oardilac/EcomStreamAnalysis/issues">Report Bug</a>
    ·
    <a href="https://github.com/oardilac/EcomStreamAnalysis/issues">Request Feature</a>
  </p>
</div>


<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

This project involves creating a data pipeline using Apache Spark and Kafka, and Docker for containerization. The pipeline involves producing and consuming messages from Kafka, and processing them using Spark. The resulting data is then saved in a Cassandra database.


### Features
The software package includes the following features:

* Jupyter Notebook: Provides an environment to run, develop, and test Python code interactively in a web browser.
* Apache Spark: A powerful open-source unified analytics engine for big data processing and machine learning.
* Kafka: A distributed event streaming platform capable of handling trillions of events a day.
* ZooKeeper: Provides centralized services for maintaining configuration information, naming, providing distributed synchronization, and providing group services.
* Spark Workers: The Docker-compose file contains two Spark workers (worker-1 and worker-2) to simulate a distributed processing environment.
* Cassandra Driver: Enables Python applications to access Cassandra in a simple and straightforward way.
* Integration with Docker: All components are containerized using Docker, which means you don't need to install any dependencies manually.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

The main frameworks and tools used in this project are:

* [Apache Kafka](https://kafka.apache.org/)
* [Apache Spark](https://spark.apache.org/)
* [Docker](https://www.docker.com/)
* [Cassandra](https://cassandra.apache.org/)


<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- GETTING STARTED -->
## Getting Started

Follow these steps to get a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites
Make sure you have the following tools installed on your machine before you start:

* Docker
* Python

### Installation

1. Clone the repo

    ```
    git clone https://github.com/oardilac/EcomStreamAnalysis.git
    ```

2. Navigate to the project directory:

    ```
    cd ProyectEndToEndData
    ```

3. Run Docker Compose to start up the services:
   ```
   docker-compose up --build
   ```

4. Access to the jupyter lab of the project with the Python files
    ```
    http://localhost:8200/lab/tree/
    ```

<!-- Usage -->
## Usage
- **docker-compose.yml**: Contains the configurations for all the services in the project.
- **code**: Directory for Jupyter notebooks and Python scripts.
- **datasets**: Directory for storing datasets.
- **project_root_directory**: Your project root directory.
- **zCustomers.cql**: Contains SQL statements for creating and populating a `customer` table.
- **code/Dockerfile**: Specifies how the Docker image for the Jupyter service is built.
- **code/z1.ProyKafkaProducer.ipynb**: A Jupyter notebook which acts as a Kafka producer, publishing data to the Kafka topic.
- **code/z2.ProyReadKafkaConsumerJoins.ipynb**: A Jupyter notebook which acts as a Kafka consumer, it joins the received data with the customer and location data.
- **code/z3.ProyReadKafkaConsumerCassandra.ipynb**: A Jupyter notebook which acts as a Kafka consumer, it consumes data from the Kafka topic and writes it into Cassandra.

<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/oardilac/EcomStreamAnalysis/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License
Distributed under the MIT License. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- CONTACT -->
## Contact

Oliver Ardila - odardilacueto@gmail.com - @oardilac

Project Link: [https://github.com/oardilac/EcomStreamAnalysis](https://github.com/oardilac/EcomStreamAnalysis)

<p align="right">(<a href="#readme-top">back to top</a>)</p>