#!/bin/sh
pdoc -c show_source_code=False --html --force -o ./docs ./pubsub_interface/todo.py
pandoc -f html -t commonmark -o ./docs/todo.md ./docs/todo.html
if diff -q README.md ./docs/todo.md >/dev/null; then
  echo "README.md is valid"
  rm -rf docs
else
  echo "Please update README.md file. See missing documentation below."
  diff -u README.md ./docs/todo.md
  rm -rf docs
  exit 1
fi