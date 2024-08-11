
# Big Data Course Study

Welcome to the repository for the Big Data course study project. This repository showcases various implementations and exercises related to big data processing and analytics, focusing on practical applications of key techniques.

## Project Overview

This repository contains scripts and examples demonstrating fundamental concepts in big data analytics. The provided examples cover a range of techniques from basic data manipulation to advanced analytics, including visualization and machine learning.

## Table of Contents

- [Introduction](#introduction)
- [Overview of Big Data Analytics](#overview-of-big-data-analytics)
- [Project Structure](#project-structure)
- [Scripts](#scripts)
- [How to Run](#how-to-run)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project is part of a course study on Big Data. It focuses on practical exercises using MapReduce and other data processing techniques to handle and analyze large datasets.

## Overview of Big Data Analytics

### Big Data Analytics Visualization

- **Big Data Analytics Visualization Design**: Techniques and tools for designing effective visualizations for large datasets.

### Big Data Analytics: Clustering

- **Big Data Clustering Implementation and Visualization**: Methods for clustering large datasets and visualizing the results to find patterns and groupings.

### Big Data Analytics: Classification

- **Big Data Classification Implementation and Visualization**: Approaches for classifying data into categories and visualizing classification results.

### Big Data Analytics: Regression

- **Big Data Regression Implementation and Visualization**: Techniques for performing regression analysis on large datasets to predict continuous outcomes.

### Big Data Analytics: Time Series

- **Big Data Time Series Implementation and Visualization**: Methods for analyzing time series data and visualizing trends and patterns over time.

### Big Data Analytics: Recommendation System

- **Big Data Recommendation System Implementation and Visualization**: Building and visualizing recommendation systems to suggest items based on user behavior.

### Big Data Analytics: Association Rules

- **Big Data Association Rules Implementation and Visualization**: Techniques for discovering association rules in datasets and visualizing these relationships.

### Big Data Analytics: Text Analysis

- **Big Data Text Analysis Implementation and Visualization**: Analyzing and visualizing textual data to extract insights and trends.

### Big Data Analytics: Graph Analysis

- **Big Data Graph Analysis Implementation and Visualization**: Techniques for analyzing and visualizing relationships and networks within large datasets.

### Big Data Analytics: Deep Learning

- **Big Data Deep Learning Implementation and Visualization**: Implementing deep learning models and visualizing their results for complex pattern recognition.

### Analyze and Summarize Key Concepts of Big Data Analytics

- **Summary**: Analyzing and summarizing the key concepts and techniques used in big data analytics.

## Project Structure

- `MapReduceJoin.py`: Performs a join operation between two types of data based on a common key.
- `MapReduceInvertedIndex.py`: Creates an inverted index from the input data.
- `MapReduceBinning.py`: Bins data into categories based on specific criteria.
- `MapReduceDistinct.py`: Extracts distinct dates from the dataset for a specific year.
- `MapReduceAverage.py`: Calculates the average number of reactions for each status type.

## How to Run

To run any of the scripts, ensure you have `mrjob` installed. You can install it using pip:

```sh
pip install mrjob
```

Run the script using the following command:

```sh
python <script_name>.py <input_file>
```

Replace `<script_name>` with the name of the script you wish to execute and `<input_file>` with the path to your data file.

## Examples

Here are a few example commands to run the provided scripts:

```sh
python MapReduceJoin.py input.csv
python MapReduceInvertedIndex.py input.csv
python MapReduceBinning.py input.csv
```

## Contributing

If you would like to contribute to this project, please fork the repository and submit a pull request. For major changes or enhancements, please open an issue to discuss your proposed changes before submitting a pull request.

## License

This project is licensed under the MIT License. All rights to the project are reserved for Dr. Sirintra Vaiwsri at King Mongkut's University of Technology North Bangkok (KMUTNB). See the [LICENSE](LICENSE) file for details.

---

Feel free to make any additional adjustments or add more details as needed!