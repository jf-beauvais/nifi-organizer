# Nifi Organizer
A tool for visually re-organizing process groups on a running Apache Nifi cluster. This tool uses the Nifi REST API to fetch the list of components in the process group, builds an in-memory graph of the components and invokes the [Graphviz](http://graphviz.org/) library to determine new positions for the components to re-organize them in a visually pleasing manner, and then updates components' positions using the Nifi API.

### Dependencies
This project was developed using:
* Python 2.7.15
* The following Python libraries:
  * pygraphviz 1.5
  * nipyapi 0.11.0

### Usage
Simply provide the script with the URL of the Nifi API on the Nifi instance, and the process group ID that you would like to organize, e.g.:
```
python organize-nifi.py localhost:9999/nifi-api c93b2b2f-ddce-41b9-bc0a-f6e633db86dc
```
