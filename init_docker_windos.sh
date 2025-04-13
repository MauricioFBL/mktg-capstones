# Windows
docker run -p 8888:8888 -v "${PWD}/data:/home/jovyan/data" -v "${PWD}/outputs:/home/jovyan/outputs" --name jupyter-notebook-container jupyter-notebook-spark
