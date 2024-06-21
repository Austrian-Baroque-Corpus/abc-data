# bin/bash

echo "add attributes"
pipenv shell
add-attributes -g "./data/editions/*.xml" -b "https://id.acdh.oeaw.ac.at/abacus"