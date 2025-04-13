# Use the jupyter/pyspark-notebook as the base image
FROM jupyter/pyspark-notebook:latest

# Add any additional configurations or dependencies if needed
RUN pip install --upgrade pip && \
    pip install pandas numpy


# USER $NB_UID