# Welcome 

This is a project I'm using as a demonstration of my Analytics Engineering skills. I live in DC and am a big fan of public transit,
so I'm accessing the WMATA Train Prediction API to observe what riders  and analyze what riders see everyday. This pulls predictions every 5 minutes, so the data is fairly 
robust and should aloow us to see how predictions change throughout the day/week.

The objective is to access the WMATA API and export the data into a JSON file and upload it into BigQuery using Python. From there I'll update the data in dbt to create 
fact and dimension tables based on Kimball's dimensional modeling. Once those tables have been created I will then leverage Looker as a visualization tool to be able to
display information about predictions and provide analysis on prediction data.
