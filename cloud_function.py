import functions_framework
from class_Handle_Upload_MetaAndTXT import Handle_Upload_MetaAndTXT
from class_gen_text import doc_gen_text

# config parameter
projectID = "" # pun-project
target_triger_path ="" # upload_test
blob_meta_path = "" # METADATA_preprocessed_file
blob_txt_path = "" # PREPROCESSED_file

# bucket_name = "pun_preprocess"
# blob_path = "upload_test/name.pdf"

@functions_framework.cloud_event
def control_event(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"] #bucket
    blob_path = data["name"] #path

    """
    this function will triger when [blob_path] have updated a new file. 
    then that file will be preprocessed and save in txt format at blob_txt_path.
    however to use those txt file in datastore we need to set metadata file in jsonl
    at blob_meta_path 
    """

    if blob_path.startswith(f"{target_triger_path}/") is True:
        #print("start process :" , blob_path)
        page = doc_gen_text(projectID = projectID, 
                            bucket_name = bucket_name,
                            blob_path = blob_path,
                            )
        page.process_OCR()
        #print("OCR success on  :" , blob_path)
        
        upload = Handle_Upload_MetaAndTXT(blob_txt_path = blob_txt_path,
                             blob_origi_path = blob_path,
                             blob_meta_path = blob_meta_path
                             )
        upload.set_params_dict(page.create_params_dict().copy())
        upload.generate_metadata()
        #print("set up meta success  :" , blob_path)
        upload.upload_txt2bucket()
        #print("upload_txt2bucket  :" , blob_path)
        upload.upload_meta2bucket()
    else : print("path is not match, recieve: ", blob_path)


#write try except timeout for 60 sec and use other method to OCR such as docAI etc.