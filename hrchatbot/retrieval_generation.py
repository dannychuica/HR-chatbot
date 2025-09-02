## Retrieve
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizableTextQuery
import os
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.models import (
    QueryType,
    QueryCaptionType,
    QueryAnswerType
)
from openai import AzureOpenAI
    
_initialized = False

def init():
    global _initialized, endpoint, credential, index_name, client
    
    if not _initialized:
        load_dotenv()
        endpoint = os.environ["AZURE_COGNITIVE_SEARCH_ENDPOINT"]
        credential = AzureKeyCredential(os.getenv("AZURE_COGNITIVE_SEARCH_KEY")) if os.getenv("AZURE_COGNITIVE_SEARCH_KEY") else DefaultAzureCredential()
        index_name = os.getenv("AZURE_SEARCH_INDEX_NAME", "int-vec")
        
        ## For Generation
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT") 
        api_key=os.getenv("AZURE_OPENAI_KEY")
        api_version=os.getenv("AZURE_OPENAI_API_VERSION")
        
        client = AzureOpenAI(
            azure_endpoint = azure_endpoint, 
            api_key = api_key,  
            api_version = api_version
        )
    
        _initialized = True
        
def retrieval_generation(query):
    
    init()
    
    # Semantic Hybrid Search
    # query = "Which is more comprehensive, Northwind Health Plus vs Northwind Standard?"
    vector_query = VectorizableTextQuery(text=query, k_nearest_neighbors=1, fields="vector", exhaustive=True)
    
    search_client = SearchClient(endpoint, index_name, credential)
    vector_query = VectorizableTextQuery(text=query, k_nearest_neighbors=1, fields="vector", exhaustive=True)
    
    results = search_client.search(  
        search_text=query,
        vector_queries=[vector_query],
        select=["parent_id", "chunk_id", "chunk"],
        query_type=QueryType.SEMANTIC,
        semantic_configuration_name='my-semantic-config',
        query_caption=QueryCaptionType.EXTRACTIVE,
        query_answer=QueryAnswerType.EXTRACTIVE,
        top=1
    )
    
    semantic_answers = results.get_answers()
    #if semantic_answers:
    #    for answer in semantic_answers:
            #if answer.highlights:
                #print(f"Semantic Answer: {answer.highlights}")
            #else:
                #print(f"Semantic Answer: {answer.text}")
            #print(f"Semantic Answer Score: {answer.score}\n")
    
    context = []
    for result in results:
        context.append({
            "parent_id": result['parent_id'],
            "chunk_id": result['chunk_id'],
            "reranker_score": result['@search.reranker_score'],
            "content": result['chunk']
        })
    
        captions = result["@search.captions"]
        if captions:
            caption = captions[0]
            #if caption.highlights:
                #print(f"Caption: {caption.highlights}\n")
            #else:
                #print(f"Caption: {caption.text}\n")
    
    
    ## Generation
    # Prompt template for injecting content and query
    prompt_template = '''
    You are a helpful assistant. Use the following context to answer the user's question.
    
    Context:
    {context}
    
    Question:
    {query}
    
    Answer:
    '''
    
    # Format the prompt
    prompt = prompt_template.format(context=context, query=query)
    #print('Prompt used for completion:')
    #print(prompt)
    
    message_text = [{"role":"system","content":prompt}]
    
    response = client.chat.completions.create(
      model=os.getenv("CHAT_COMPLETION_NAME"), # model = "deployment_name"
      messages = message_text,
    )
    
    return response.choices[0].message.content 
