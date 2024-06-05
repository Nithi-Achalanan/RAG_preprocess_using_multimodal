from google.cloud import storage
from pdf2image import convert_from_bytes
import base64
import io
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import vertexai.preview.generative_models as generative_models
from concurrent.futures import ThreadPoolExecutor

class doc_gen_text:
    def __init__(self, projectID: str, bucket_name: str, blob_path: str, projectLocate: str = "asia-southeast1", overlap_size: int = 0):
        self.projectID = projectID
        self.bucket_name = bucket_name
        self.blob_path = blob_path # path exclude bucket but include 
        self.projectLocate = projectLocate
        self.overlap_size = overlap_size
        self.OCR_result = None

    def bucket2PNGbase64(self):
        """
            get the pdf in gcs bucket and return base64 png 
            input : projectID -> gcp project srting
                    bucket_name -> bucket
                    blob_path -> path/item.format
                    
            output : base64_images -> ["","","",""] PNGbase64 of each page in document
        """
        # Initialize a client with the specified project
        storage_client = storage.Client(project =self.projectID)

        # Specify the bucket name

        # Create a bucket object for our bucket
        bucket = storage_client.get_bucket(self.bucket_name)
        # Create a blob object from the filepath
        blob = bucket.blob(self.blob_path)

        # Download the blob's content as a byte stream
        pdf_bytes = blob.download_as_bytes()

        # Convert PDF bytes to images
        images = convert_from_bytes(pdf_bytes)

        # Initialize a list to store base64 encoded images
        base64_images = []

        # Convert each image to base64 format
        for i, image in enumerate(images):
            # Convert PIL image to bytes
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_byte_arr = img_byte_arr.getvalue()
        
            # Convert bytes to base64
            base64_image = base64.b64encode(img_byte_arr).decode('utf-8')
            
            # Append to the list
            base64_images.append(base64_image)
        return base64_images
    def generate_parallel(self, prompt):
        """
            this function sent request to LLM in parallel 
            
            input : base64_images_dict -> [{ "no_page" : int , "image" : base64 },,,,,,,]
        
        """
        vertexai.init(project=self.projectID, location=self.projectLocate)
        model = GenerativeModel("gemini-1.5-flash-001")
        
        #base64_images_dict  { "image" : , "no_page" :}  
        prompt = prompt*len(self.base64_images_dict)
        # print("list prompt size" ,len(prompt))
        
        # Use a ThreadPoolExecutor for parallel execution
        with ThreadPoolExecutor(max_workers=len(self.base64_images_dict)) as executor:
            futures = []
            ans = []
            ans_dict = {}
            for image_data, image_desc in zip(self.base64_images_dict, prompt):
                # print(image_desc)
                future = executor.submit(self._generate_image_text, model, image_data, image_desc)
                futures.append(future)

            # Gather results from parallel tasks
            for future in futures:
                result_text = future.result()
                # print(result_text)
                ans.append(result_text)
                key = list(result_text.keys())[0]
                ans_dict[key] = result_text[key]
            return(ans_dict)
    def _generate_image_text(self,model, image_data, image_description):
        base64_images = image_data["image"]
        no_page = image_data["no_page"] 
    
        safety_settings = {
                generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
            }
        
        image = Part.from_data(mime_type="image/png", data=base64.b64decode(base64_images))
        generation_config = {
            "max_output_tokens": 8192,
            "temperature": 0.5,
            "top_p": 0.5,
        }
        responses = model.generate_content(
            [image_description, image,],
            generation_config=generation_config,
            stream=True,
        )

        text = ""
        for response in responses:
            text += response.text
        return {str(no_page) : text ,}
    
    def transform_parallel_input(self): 
        """transform list to dict with page_id
            input : [bas64 1 , base64 2, ,,,]
            output :  -> [{ "no_page" : int , "image" : base64 },,,,,,,]
        """
        list_intput = self.base64_images_input
        ans_list = []
        for i,item in enumerate(list_intput): #[{}, {}, {},,, ]
            ans_list.append({"image" : item, "no_page" : i})
        return ans_list
    def overlap_page(self, data_dict):
        data_dict_overlap = data_dict.copy()
        for no_page in range(1,len(data_dict)-1):
            no_page_pre = str(no_page-1)
            no_page_post = str(no_page+1)
            data_dict_overlap[str(no_page)] = data_dict[no_page_pre][-self.overlap_size:]+" "+data_dict[str(no_page)] + data_dict[no_page_post][0:self.overlap_size] 
        data_dict_overlap["0"] = data_dict["0"] + data_dict["1"][0:self.overlap_size]
        data_dict_overlap[str(len(data_dict)-1)] = data_dict[str( len(data_dict)-2 )][-self.overlap_size:] + data_dict[str(len(data_dict)-1)]
        return data_dict_overlap
    
    def process_OCR(self):
        self.base64_images_input = self.bucket2PNGbase64()
        # prompt = """หน้าที่ของคุณคือแปลงรูปแบบเอกสารที่ได้รับจากรูปที่เป็นเอกสารที่ถูกสแกนเก็บไว้ในรูปแบบของรูปภาพให้เป็นบทความภาษาไทย โดยงานของคุณจะแบ่งออกเป็นสามกรณีดังนี้:
        #     1. ให้ใช้เทคโนโลยี OCR เพื่ออ่านข้อความและเขียนให้เหมือนต้นฉบับโดยไม่แก้ไขข้อมูลหรือคำใดเด็ดขาดๆ
        #     2. เมื่อพบรูปขั้นตอนหรือกระบวนการในเอกสาร: ให้กำหนดหน้าที่ให้ผู้ใช้อธิบายรูปภาพให้เข้าใจง่ายและชัดเจน ถ้าเป็นกระบวนการบางอย่างให้อธิบายกระบวนการนั้น
        #     3. เมื่อพบตารางหรือข้อมูลอื่นๆในเอกสาร: ให้แปลงข้อมูลให้อยู่ในรูปแบบ CSV เพื่อความสะดวกในการใช้งานต่อไป
        #     เอกสาร :
        #     """
        prompt = """คุณเป็นนักเขียนหน้าที่ของคุณคือแปลงเอกสารที่ได้รับให้เป็นตัวหนังสือ โดยแบ่งออกเป็น 2 กรณีดังนี้:
            1. เมื่อพบข้อความในเอกสาร: ให้ใช้เทคโนโลยี OCR เพื่ออ่านข้อความและเขียนให้เหมือนต้นฉบับโดยไม่แก้ไขข้อมูลหรือคำใดเด็ดขาดๆ
            2. เมื่อพบ flow chart ในเอกสาร:  ให้อธิบายกระบวนการนั้นอย่างละเอียด
            ในเอกสารอาจจะประกอบด้วยมากกว่า 1 ประเภท ให้ทำงานจากบนลงล่าง และนำข้อความที่ได้มาต่อกัน"""
        prompt = [prompt]
        list_input = self.transform_parallel_input()
        self.base64_images_dict = list_input
        # print("base64_images_dict size :",len(self.base64_images_dict))
        response = self.generate_parallel(prompt=prompt)
        # print(response)
        # print(len(response))


        if self.overlap_size == 0 :
            pass
        else :
            response_ = self.overlap_page(data_dict = response )
            response = response_.copy()
        self.OCR_result = response
    def create_params_dict(self):
        return {
            "OCR_result" :  self.OCR_result,
            "projectID" : self.projectID,
            "bucket_name" : self.bucket_name,
            "blob_path" : self.blob_path,
            "projectLocate" : self.projectLocate,
            "overlap_size" : self.overlap_size,
        }
        
# page = doc_gen_text(projectID = "" #pun-project, 
#                     bucket_name = "" #pun_preprocess,
#                     blob_path = "" #upload_test/text.pdf,
#                     )
# page.process_OCR()
# page.create_params_dict()   
        
