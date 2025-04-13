# Linux y macOS
docker run -p 8888:8888 -v $(pwd)/inputs:/home/jovyan/inputs -v $(pwd)/outputs:/home/jovyan/outputs  --name jupyter-notebook-container jupyter-notebook-spark
