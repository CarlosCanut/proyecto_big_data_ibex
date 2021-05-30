#!/usr/bin/env bash

declare -A animals
animals=( ["moo"]="cow" ["woof"]="dog")
for animal in animals; do
	echo $animal
done

