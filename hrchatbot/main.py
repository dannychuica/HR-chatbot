from .load_data_create_index import load_data_create_index
from .retrieval_generation import retrieval_generation

print("Just a second... HR CHatbot is preparing to answer your questions.\n")

load_data_create_index();

while True:
    user_input = input("Question: ")
    if user_input.lower() == 'exit':
        print("Goodbye!")
        break
    response = retrieval_generation(user_input)
    
    print(f"Answer:{response}\n")

