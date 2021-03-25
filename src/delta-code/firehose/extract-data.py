import json
import re

# path = '/opt/bitnami/spark/datasets/thing_payload_stream'
# extract_file = '/opt/bitnami/spark/datasets/thing_payload_stream-extract.txt'

path = '/opt/bitnami/spark/datasets/thing_outputs_stream'
extract_file = '/opt/bitnami/spark/datasets/thing_outputs_stream-extract.txt'
# with open('/opt/bitnami/spark/datasets/thing_payload_stream') as inp:
#     s = inp.read().strip()

# jsons = []

# start, end = s.find('{'), s.find('}')
# while True:
#     try:
#         jsons.append((s[start:end + 1]))
#     except ValueError:
#         end = end + 1 + s[end + 1:].find('}')
#     else:
#         s = s[end + 1:]
#         if not s:
#             break
#         start, end = s.find('{'), s.find('}')

# for x  in jsons:
    # print(x)


# file1 = open(extract_file, "w")
# # L = ["This is Delhi \n", "This is Paris \n", "This is London \n"]
# for x  in jsons:
#     print(x)
#     file1.write(x + "\n")
#     # print(x)
# # file1.writelines(jsons)
# file1.close()

with open(path) as file:
    json_string = file.read()

json_objects = re.sub('}\s*{', '}|!|{', json_string).split('|!|')
# replace |!| with whatever suits you best

# for json_object in json_objects:
#     print(json.loads(json_object))

file1 = open(extract_file, "w")
# L = ["This is Delhi \n", "This is Paris \n", "This is London \n"]
for x  in json_objects:
    print(x)
    file1.write(x + "\n")
    # print(x)
# file1.writelines(jsons)
file1.close()