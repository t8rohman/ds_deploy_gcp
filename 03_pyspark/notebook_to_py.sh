# script to convert notebook to python file
# using nbconvert

cwd="xxx"
notebook_path="xxx"

cd "$cwd"
jupyter nbconvert --to script "$notebook_path"