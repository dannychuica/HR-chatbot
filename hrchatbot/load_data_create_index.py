from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
import os

def load_data_create_index():
    load_dotenv(override=True) # take environment variables from .env.
    
    # Variables not used here do not need to be updated in your .env file
    endpoint = os.environ["AZURE_COGNITIVE_SEARCH_ENDPOINT"]
    credential = AzureKeyCredential(os.getenv("AZURE_COGNITIVE_SEARCH_KEY")) if os.getenv("AZURE_COGNITIVE_SEARCH_KEY") else DefaultAzureCredential()
    index_name = os.getenv("AZURE_SEARCH_INDEX_NAME", "int-vec")
    blob_connection_string = os.getenv("BLOB_CONNECTION_STRING")
    # search blob datasource connection string is optional - defaults to blob connection string
    # This field is only necessary if you are using MI to connect to the data source
    # https://learn.microsoft.com/azure/search/search-howto-indexing-azure-blob-storage#supported-credentials-and-connection-strings
    # search_blob_connection_string = os.getenv("SEARCH_BLOB_DATASOURCE_CONNECTION_STRING", blob_connection_string)
    blob_container_name = os.getenv("BLOB_CONTAINER_NAME", "int-vec")
    azure_openai_endpoint = os.environ["AZURE_OPENAI_ENDPOINT"]
    azure_openai_key = os.getenv("AZURE_OPENAI_KEY")
    azure_openai_embedding_deployment = os.getenv("EMBEDDING_MODEL_NAME", "text-embedding-3-large")
    azure_openai_model_name = os.getenv("EMBEDDING_MODEL_NAME", "text-embedding-3-large")
    azure_openai_model_dimensions = int(os.getenv("AZURE_OPENAI_EMBEDDING_DIMENSIONS", 1536))
    # This field is only necessary if you want to use OCR to scan PDFs in the datasource or use the Document Layout skill without a key
    azure_ai_services_endpoint = os.getenv("AZURE_COGS_ENDPOINT", "")
    # This field is only necessary if you want to use OCR to scan PDFs in the data source or use the Document Layout skill and you want to authenticate using a key to Azure AI Services
    azure_ai_services_key = os.getenv("AZURE_COGS_KEY", "")
    
    # set USE_OCR to enable OCR to add page numbers. It cannot be combined with the document layout skill
    use_ocr = os.getenv("USE_OCR", "false") == "true"
    # set USE_LAYOUT to enable Document Intelligence Layout skill for chunking by markdown. It cannot be combined with the built-in OCR
    use_document_layout = os.getenv("USE_LAYOUT", "false") == "true"
    # set USE_MARKDOWN to enable parsing markdown files in the blob container. It cannot be combined with the built-in OCR or document layout skill
    use_markdown = os.getenv("USE_MARKDOWN", "false") == "true"
    # Deepest nesting level in markdown that should be considered. See https://learn.microsoft.com/azure/search/cognitive-search-skill-document-intelligence-layout to learn more
    document_layout_depth = os.getenv("LAYOUT_MARKDOWN_HEADER_DEPTH", "h3")
    # OCR must be used to add page numbers
    add_page_numbers = use_ocr
    
    count_enabled = sum([use_ocr, use_document_layout, use_markdown])
    if count_enabled >= 2:
        raise Exception(f"Please enable only one of OCR, Layout or Markdown.")
    
    #print(f"blob_container_name is {blob_container_name}")
    #print(f"azure_openai_endpoint is {azure_openai_endpoint}")
    #print(f"endpoint is {endpoint}")
    #print(f"azure_ai_services_endpoint is {azure_ai_services_endpoint}")
    #print(f"add_page_numbers is {add_page_numbers}")
       
    ## Connect to Blob Storage and load documents
    
    from azure.storage.blob import BlobServiceClient  
    import glob
    
    sample_docs_directory = os.path.join( "data", "documents")
    sample_ocr_docs_directory = os.path.join( "data", "documents")
    sample_layout_docs_directory = os.path.join( "data", "documents")
    sample_markdown_docs_directory = os.path.join( "data", "documents")
    
    def upload_sample_documents(
            blob_connection_string: str,
            blob_container_name: str,
            documents_directory: str,
            # Set to false if you want to use credentials included in the blob connection string
            # Otherwise your identity will be used as credentials
            use_user_identity: bool = False
        ):
            # Connect to Blob Storage
            blob_service_client = BlobServiceClient.from_connection_string(logging_enable=True, conn_str=blob_connection_string, credential=DefaultAzureCredential() if use_user_identity else None)
            container_client = blob_service_client.get_container_client(blob_container_name)
            if not container_client.exists():
                container_client.create_container()
    
            files = glob.glob(os.path.join(documents_directory, '*'))
            for file in files:
                with open(file, "rb") as data:
                    name = os.path.basename(file)
                    if not container_client.get_blob_client(name).exists():
                        container_client.upload_blob(name=name, data=data)
    
    docs_directory = sample_docs_directory
    
    if use_ocr:
        docs_directory = sample_ocr_docs_directory
    elif use_document_layout:
        docs_directory = sample_layout_docs_directory
    elif use_markdown:
        docs_directory = sample_markdown_docs_directory
    
    upload_sample_documents(
        blob_connection_string=blob_connection_string,
        blob_container_name=blob_container_name,
        documents_directory = docs_directory)
    
    #print(f"Setup sample data in {blob_container_name}")
    
    
    ## Create a blob data source connector on Azure AI Search
    from azure.search.documents.indexes import SearchIndexerClient
    from azure.search.documents.indexes.models import (
        SearchIndexerDataContainer,
        SearchIndexerDataSourceConnection
    )
    from azure.search.documents.indexes.models import NativeBlobSoftDeleteDeletionDetectionPolicy
    
    # Create a data source 
    indexer_client = SearchIndexerClient(endpoint, credential)
    container = SearchIndexerDataContainer(name=blob_container_name)
    data_source_connection = SearchIndexerDataSourceConnection(
        name=f"{index_name}-blob",
        type="azureblob",
        connection_string=blob_connection_string,
        container=container,
        data_deletion_detection_policy=NativeBlobSoftDeleteDeletionDetectionPolicy() if not use_markdown else None
    )
    data_source = indexer_client.create_or_update_data_source_connection(data_source_connection)
    
    #print(f"Data source '{data_source.name}' created or updated")
    
    ## Create a search index
    from azure.search.documents.indexes import SearchIndexClient
    from azure.search.documents.indexes.models import (
        SearchField,
        SearchFieldDataType,
        VectorSearch,
        HnswAlgorithmConfiguration,
        VectorSearchProfile,
        AzureOpenAIVectorizer,
        AzureOpenAIVectorizerParameters,
        SemanticConfiguration,
        SemanticSearch,
        SemanticPrioritizedFields,
        SemanticField,
        SearchIndex
    )
    
    # Create a search index  
    index_client = SearchIndexClient(endpoint=endpoint, credential=credential)  
    fields = [  
        SearchField(name="parent_id", type=SearchFieldDataType.String, sortable=True, filterable=True, facetable=True),  
        SearchField(name="title", type=SearchFieldDataType.String),  
        SearchField(name="chunk_id", type=SearchFieldDataType.String, key=True, sortable=True, filterable=True, facetable=True, analyzer_name="keyword"),  
        SearchField(name="chunk", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False),  
        SearchField(name="vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single), vector_search_dimensions=azure_openai_model_dimensions, vector_search_profile_name="myHnswProfile"),  
    ]
    
    if add_page_numbers:
        fields.append(
            SearchField(name="page_number", type=SearchFieldDataType.String, sortable=True, filterable=True, facetable=False)
        )
    
    if use_document_layout or use_markdown:
        fields.extend([
            SearchField(name="header_1", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False),
            SearchField(name="header_2", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False),
            SearchField(name="header_3", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False)
        ])
      
    # Configure the vector search configuration  
    vector_search = VectorSearch(  
        algorithms=[  
            HnswAlgorithmConfiguration(name="myHnsw"),
        ],  
        profiles=[  
            VectorSearchProfile(  
                name="myHnswProfile",  
                algorithm_configuration_name="myHnsw",  
                vectorizer_name="myOpenAI",  
            )
        ],  
        vectorizers=[  
            AzureOpenAIVectorizer(  
                vectorizer_name="myOpenAI",  
                kind="azureOpenAI",  
                parameters=AzureOpenAIVectorizerParameters(  
                    resource_url=azure_openai_endpoint,  
                    deployment_name=azure_openai_embedding_deployment,
                    model_name=azure_openai_model_name,
                    api_key=azure_openai_key,
                ),
            ),  
        ],  
    )  
      
    semantic_config = SemanticConfiguration(  
        name="my-semantic-config",  
        prioritized_fields=SemanticPrioritizedFields(  
            content_fields=[SemanticField(field_name="chunk")],
            title_field=SemanticField(field_name="title")
        ),  
    )
      
    # Create the semantic search with the configuration  
    semantic_search = SemanticSearch(configurations=[semantic_config])  
      
    # Create the search index
    index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search, semantic_search=semantic_search)  
    result = index_client.create_or_update_index(index)  
    #print(f"{result.name} created")  
    
    
    ## Create a skillset
    from azure.search.documents.indexes.models import (
        SplitSkill,
        InputFieldMappingEntry,
        OutputFieldMappingEntry,
        AzureOpenAIEmbeddingSkill,
        OcrSkill,
        SearchIndexerIndexProjection,
        SearchIndexerIndexProjectionSelector,
        SearchIndexerIndexProjectionsParameters,
        IndexProjectionMode,
        SearchIndexerSkillset,
        AIServicesAccountKey,
        AIServicesAccountIdentity,
        DocumentIntelligenceLayoutSkill
    )
    
    # Create a skillset name 
    skillset_name = f"{index_name}-skillset"
    
    def create_ocr_skillset():
        ocr_skill = OcrSkill(
            description="OCR skill to scan PDFs and other images with text",
            context="/document/normalized_images/*",
            line_ending="Space",
            default_language_code="en",
            should_detect_orientation=True,
            inputs=[
                InputFieldMappingEntry(name="image", source="/document/normalized_images/*")
            ],
            outputs=[
                OutputFieldMappingEntry(name="text", target_name="text"),
                OutputFieldMappingEntry(name="layoutText", target_name="layoutText")
            ]
        )
    
        split_skill = SplitSkill(  
            description="Split skill to chunk documents",  
            text_split_mode="pages",  
            context="/document/normalized_images/*",  
            maximum_page_length=2000,  
            page_overlap_length=500,  
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/normalized_images/*/text"),  
            ],  
            outputs=[  
                OutputFieldMappingEntry(name="textItems", target_name="pages")  
            ]
        )
    
        embedding_skill = AzureOpenAIEmbeddingSkill(  
            description="Skill to generate embeddings via Azure OpenAI",  
            context="/document/normalized_images/*/pages/*",  
            resource_url=azure_openai_endpoint,  
            deployment_name=azure_openai_embedding_deployment,  
            model_name=azure_openai_model_name,
            dimensions=azure_openai_model_dimensions,
            api_key=azure_openai_key,  
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/normalized_images/*/pages/*"),  
            ],  
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="vector")  
            ]
        )
    
        index_projections = SearchIndexerIndexProjection(  
            selectors=[  
                SearchIndexerIndexProjectionSelector(  
                    target_index_name=index_name,  
                    parent_key_field_name="parent_id",  
                    source_context="/document/normalized_images/*/pages/*",  
                    mappings=[
                        InputFieldMappingEntry(name="chunk", source="/document/normalized_images/*/pages/*"),  
                        InputFieldMappingEntry(name="vector", source="/document/normalized_images/*/pages/*/vector"),
                        InputFieldMappingEntry(name="title", source="/document/metadata_storage_name"),
                        InputFieldMappingEntry(name="page_number", source="/document/normalized_images/*/pageNumber")
                    ]
                )
            ],  
            parameters=SearchIndexerIndexProjectionsParameters(  
                projection_mode=IndexProjectionMode.SKIP_INDEXING_PARENT_DOCUMENTS  
            )  
        )
    
        skills = [ocr_skill, split_skill, embedding_skill]
    
        return SearchIndexerSkillset(  
            name=skillset_name,  
            description="Skillset to chunk documents and generating embeddings",  
            skills=skills,  
            index_projection=index_projections,
            cognitive_services_account=AIServicesAccountKey(key=azure_ai_services_key, subdomain_url=azure_ai_services_endpoint) if azure_ai_services_key else AIServicesAccountIdentity(identity=None, subdomain_url=azure_ai_services_endpoint)
        )
    
    def create_layout_skillset():
        layout_skill = DocumentIntelligenceLayoutSkill(
            description="Layout skill to read documents",
            context="/document",
            output_mode="oneToMany",
            markdown_header_depth="h3",
            inputs=[
                InputFieldMappingEntry(name="file_data", source="/document/file_data")
            ],
            outputs=[
                OutputFieldMappingEntry(name="markdown_document", target_name="markdownDocument")
            ]
        )
    
        split_skill = SplitSkill(  
            description="Split skill to chunk documents",  
            text_split_mode="pages",  
            context="/document/markdownDocument/*",  
            maximum_page_length=2000,  
            page_overlap_length=500,  
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/markdownDocument/*/content"),  
            ],  
            outputs=[  
                OutputFieldMappingEntry(name="textItems", target_name="pages")  
            ]
        )
    
        embedding_skill = AzureOpenAIEmbeddingSkill(  
            description="Skill to generate embeddings via Azure OpenAI",  
            context="/document/markdownDocument/*/pages/*",  
            resource_url=azure_openai_endpoint,  
            deployment_name=azure_openai_embedding_deployment, 
            model_name=azure_openai_model_name,
            dimensions=azure_openai_model_dimensions,
            api_key=azure_openai_key,
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/markdownDocument/*/pages/*"),  
            ],  
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="vector")  
            ]
        )
    
        index_projections = SearchIndexerIndexProjection(  
            selectors=[  
                SearchIndexerIndexProjectionSelector(  
                    target_index_name=index_name,  
                    parent_key_field_name="parent_id",  
                    source_context="/document/markdownDocument/*/pages/*",  
                    mappings=[
                        InputFieldMappingEntry(name="chunk", source="/document/markdownDocument/*/pages/*"),  
                        InputFieldMappingEntry(name="vector", source="/document/markdownDocument/*/pages/*/vector"),
                        InputFieldMappingEntry(name="title", source="/document/metadata_storage_name"),
                        InputFieldMappingEntry(name="header_1", source="/document/markdownDocument/*/sections/h1"),
                        InputFieldMappingEntry(name="header_2", source="/document/markdownDocument/*/sections/h2"),
                        InputFieldMappingEntry(name="header_3", source="/document/markdownDocument/*/sections/h3"),
                    ]
                )
            ],  
            parameters=SearchIndexerIndexProjectionsParameters(  
                projection_mode=IndexProjectionMode.SKIP_INDEXING_PARENT_DOCUMENTS  
            )  
        )
    
        skills = [layout_skill, split_skill, embedding_skill]
    
        return SearchIndexerSkillset(  
            name=skillset_name,  
            description="Skillset to chunk documents and generating embeddings",  
            skills=skills,  
            index_projection=index_projections,
            cognitive_services_account=AIServicesAccountKey(key=azure_ai_services_key, subdomain_url=azure_ai_services_endpoint) if azure_ai_services_key else AIServicesAccountIdentity(identity=None, subdomain_url=azure_ai_services_endpoint)
        )
    
    def create_markdown_skillset():
        split_skill = SplitSkill(  
            description="Split skill to chunk documents",  
            text_split_mode="pages",  
            context="/document",  
            maximum_page_length=2000,  
            page_overlap_length=500,  
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/content"),  
            ],  
            outputs=[  
                OutputFieldMappingEntry(name="textItems", target_name="pages")  
            ]
        )
    
        embedding_skill = AzureOpenAIEmbeddingSkill(  
            description="Skill to generate embeddings via Azure OpenAI",  
            context="/document/pages/*",  
            resource_url=azure_openai_endpoint,  
            deployment_name=azure_openai_embedding_deployment,
            model_name=azure_openai_model_name,
            dimensions=azure_openai_model_dimensions,
            api_key=azure_openai_key,  
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/pages/*"),  
            ],  
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="vector")  
            ]
        )
    
        index_projections = SearchIndexerIndexProjection(  
            selectors=[  
                SearchIndexerIndexProjectionSelector(  
                    target_index_name=index_name,  
                    parent_key_field_name="parent_id",  
                    source_context="/document/pages/*",  
                    mappings=[
                        InputFieldMappingEntry(name="chunk", source="/document/pages/*"),  
                        InputFieldMappingEntry(name="vector", source="/document/pages/*/vector"),
                        InputFieldMappingEntry(name="title", source="/document/metadata_storage_name"),
                        InputFieldMappingEntry(name="header_1", source="/document/sections/h1"),
                        InputFieldMappingEntry(name="header_2", source="/document/sections/h2"),
                        InputFieldMappingEntry(name="header_3", source="/document/sections/h3"),
                    ]
                )
            ],  
            parameters=SearchIndexerIndexProjectionsParameters(  
                projection_mode=IndexProjectionMode.SKIP_INDEXING_PARENT_DOCUMENTS  
            )  
        )
    
        skills = [split_skill, embedding_skill]
    
        return SearchIndexerSkillset(  
            name=skillset_name,  
            description="Skillset to chunk documents and generating embeddings",  
            skills=skills,  
            index_projection=index_projections,
            cognitive_services_account=AIServicesAccountKey(key=azure_ai_services_key, subdomain_url=azure_ai_services_endpoint) if azure_ai_services_key else AIServicesAccountIdentity(identity=None, subdomain_url=azure_ai_services_endpoint)
        )
    
    def create_skillset():
        split_skill = SplitSkill(  
            description="Split skill to chunk documents",  
            text_split_mode="pages",  
            context="/document",  
            maximum_page_length=2000,  
            page_overlap_length=500,  
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/content"),  
            ],  
            outputs=[  
                OutputFieldMappingEntry(name="textItems", target_name="pages")  
            ]
        )
    
        embedding_skill = AzureOpenAIEmbeddingSkill(  
            description="Skill to generate embeddings via Azure OpenAI",  
            context="/document/pages/*",  
            resource_url=azure_openai_endpoint,  
            deployment_name=azure_openai_embedding_deployment,  
            model_name=azure_openai_model_name,
            dimensions=azure_openai_model_dimensions,
            api_key=azure_openai_key,  
            inputs=[  
                InputFieldMappingEntry(name="text", source="/document/pages/*"),  
            ],  
            outputs=[
                OutputFieldMappingEntry(name="embedding", target_name="vector")  
            ]
        )
    
        index_projections = SearchIndexerIndexProjection(  
            selectors=[  
                SearchIndexerIndexProjectionSelector(  
                    target_index_name=index_name,  
                    parent_key_field_name="parent_id",  
                    source_context="/document/pages/*",  
                    mappings=[
                        InputFieldMappingEntry(name="chunk", source="/document/pages/*"),  
                        InputFieldMappingEntry(name="vector", source="/document/pages/*/vector"),
                        InputFieldMappingEntry(name="title", source="/document/metadata_storage_name")
                    ]
                )
            ],  
            parameters=SearchIndexerIndexProjectionsParameters(  
                projection_mode=IndexProjectionMode.SKIP_INDEXING_PARENT_DOCUMENTS  
            )  
        )
    
        skills = [split_skill, embedding_skill]
    
        return SearchIndexerSkillset(  
            name=skillset_name,  
            description="Skillset to chunk documents and generating embeddings",  
            skills=skills,  
            index_projection=index_projections
        )
    
    skillset = create_ocr_skillset() if use_ocr else create_layout_skillset() if use_document_layout else create_markdown_skillset() if use_markdown else create_skillset()
      
    client = SearchIndexerClient(endpoint, credential)  
    client.create_or_update_skillset(skillset)  
    #print(f"{skillset.name} created")  
    
    ## Create an Indexer
    from azure.search.documents.indexes.models import (
        SearchIndexer,
        IndexingParameters,
        IndexingParametersConfiguration,
        BlobIndexerImageAction,
    )
    
    # Create an indexer  
    indexer_name = f"{index_name}-indexer"  
    
    indexer_parameters = None
    if use_ocr:
        indexer_parameters = IndexingParameters(
            configuration=IndexingParametersConfiguration(
                image_action=BlobIndexerImageAction.GENERATE_NORMALIZED_IMAGE_PER_PAGE,
                query_timeout=None))
    elif use_document_layout:
        indexer_parameters = IndexingParameters(
            configuration=IndexingParametersConfiguration(
                allow_skillset_to_read_file_data=True,
                query_timeout=None))
    elif use_markdown:
        indexer_parameters = IndexingParameters(
            configuration=IndexingParametersConfiguration(
                parsing_mode="markdown",
                markdown_parsing_submode="oneToMany",
                markdown_header_depth=document_layout_depth,
                query_timeout=None))
    
    indexer = SearchIndexer(  
        name=indexer_name,  
        description="Indexer to index documents and generate embeddings",  
        skillset_name=skillset_name,  
        target_index_name=index_name,  
        data_source_name=data_source.name,
        parameters=indexer_parameters
    )  
    
    indexer_client = SearchIndexerClient(endpoint, credential)  
    indexer_result = indexer_client.create_or_update_indexer(indexer)  
      
    # Run the indexer  
    indexer_client.run_indexer(indexer_name)  
    #print(f'Indexer {indexer_name} is created and running. If queries return no results, please wait a bit and try again.')  

