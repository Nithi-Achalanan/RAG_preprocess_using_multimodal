import json
from google.cloud import storage
class Handle_Upload_MetaAndTXT:
    def __init__(self,blob_origi_path, blob_txt_path,blob_meta_path , projectID=None, bucket_name=None, projectLocate=None, OCR_result=None, overlap_size=None):
        self.projectID = projectID
        self.bucket_name = bucket_name
        self.blob_origi_path = blob_origi_path #include file name but not include bucket
        self.blob_txt_path = blob_txt_path # include file name but not include bucket
        self.blob_txt_folder_path = None #needed path to TXT(OCR) file
        self.blob_txt_name = None #needed
        self.blob_meta_path = blob_meta_path # not include bucket and file name
        self.projectLocate = projectLocate
        self.OCR_result = OCR_result
        self.result = {}
        self.overlap_size = overlap_size
        self.separate_filename_txt()
        
    def set_params_dict(self, params_dict):
        for key, value in params_dict.items():
            if hasattr(self, key) and getattr(self,key) == None:
                if key == "OCR_result" :
                    self.OCR_result = value.copy()
                else :
                    setattr(self, key, value) 
    def separate_filename_txt(self):
        path_components = self.blob_txt_path.split("/")
        self.blob_txt_folder_path = "/".join(path_components[:-1])
        self.blob_txt_name = path_components[-1]
        
    def generate_metadata(self):
        txt_name = self.blob_txt_name.split(".")[0]
        for page in self.OCR_result:
            txt = self.OCR_result[page]
            # self.OCR_result[page]
            page_description = {
                    "structData": { # ใส่ meta อะไรก็ได้
                        "description": "gemini_ocr",
                        "uri": f"https://storage.googleapis.com/{self.bucket_name}/{self.blob_origi_path}#page={page}",
                        # structData[url] คือไฟล์ต้นฉบับ
                        "title": f"{self.blob_origi_path.split('/')[-1]}", # / ข้างหลัง ตัวสุดท้าย
                        "overlap_size" : f"{self.overlap_size}",
                        "page" : f"{page}"
                    },
                    "content": {
                        "mimeType": "text/plain",
                        "uri": f"gs://{self.bucket_name}/{self.blob_txt_folder_path}/{txt_name}-P{page}.txt"
                    # content["url"] คือ .txt สำหรับ datastore
                    }
                }
            self.result[f"{page}"] = {"meta":page_description, "txt" : txt}.copy()
    #def upload_meta2bucket(self):
        
    def upload_txt2bucket(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)

        for page in self.result:
            URL = self.result[page]["meta"]["content"]["uri"]
            txt_name = URL.split("/")[-1].split(".")[0] + ".txt"
            storage_client = storage.Client()
            blob = bucket.blob(f"{self.blob_txt_folder_path}/{txt_name}")
            with blob.open("w") as file:
                file.write(self.result[page]["txt"])
    
    def upload_meta2bucket(self): # to use self.result and upload it to meta_path
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)

        for page in self.result:
            URL = self.result[page]["meta"]["content"]["uri"]
            txt_name = URL.split("/")[-1]
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.bucket_name)
            blob = bucket.blob(f"{self.blob_meta_path}/{txt_name}.jsonl")
            with blob.open("w") as file:
                json_line = json.dumps(self.result[page]["meta"])
                file.write(json_line + "\n")
                           
# Get the bucket.
# Define the blob (file) path in the bucket.
# Write each dictionary as a JSON object on a new line in the file.
    
# A = Handle_Upload_MetaAndTXT(blob_txt_path = "wha_gen_demo/wha_topic3/txt/Thailaw/00016937.pdf",
#                              blob_origi_path = "wha_gen_demo/wha_topic3/origi/Thailaw/00016937.pdf",
#                              blob_meta_path = "wha_gen_demo/wha_topic3/metadata/Thailaw/00016937.pdf"
#                              )
# A.set_params_dict(page.create_params_dict())
# A.generate_metadata()
