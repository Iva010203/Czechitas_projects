import json

# Open the file in read mode
with open('Alice.txt', 'r', encoding='utf-8') as file:
    # Read the whole text
    text = file.read()

# Remove whitespces and new lines, convert to lowercase
formated_text = text.replace('\n', '').replace(' ', '').lower()

# define a dictionary to hold the frequancy of each character
char_count = {}
for char in formated_text:
    if char in char_count:
        char_count[char] += 1
    else:
        char_count[char] = 1

# Sort the dictionary by keys (characters)
sorted_by_key = dict(sorted(char_count.items()))

# Convert the dictionary to a JSON
json_output = json.dumps(sorted_by_key, indent=4, ensure_ascii=False)

# write to a file
with open('hw01_output.json', 'w', encoding='utf-8') as json_file:
    json_file.write(json_output)
