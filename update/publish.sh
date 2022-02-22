#!/usr/bin/env bash

filename=$(echo "$1" | sed -e "s/ipynb/html/g")
jupyter nbconvert --to html "$1" --TagRemovePreprocessor.remove_input_tags='{"no_input"}' --TagRemovePreprocessor.remove_all_outputs_tags='{"no_output"}' --TagRemovePreprocessor.remove_cell_tags='{"no_display"}' --no-prompt --template "update" --stdout > "docs/index.html"
