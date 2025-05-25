# Linux y macOS
docker run --rm \
  -p 8888:8888 \
  -v $(pwd)/data:/home/jovyan/data \
  -v $(pwd)/outputs:/home/jovyan/outputs \
  --name jupyter-notebook-container \
  jupyter-notebook-spark
