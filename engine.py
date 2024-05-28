from google.cloud import storage
from pdf2image import convert_from_bytes
import base64
import io
import vertexai
from vertexai.generative_models import GenerativeModel, Part, FinishReason
import vertexai.preview.generative_models as generative_models
from concurrent.futures import ThreadPoolExecutor

"""this function have a ability to get bucket interupt 
first read the updated file
then transform it with gemini
lastly a transformed data (txt) other bucket"""


def bucket2PNGbase64(projectID,bucket_name,blob_path): 
    """
        get the pdf in gcs bucket and return base64 png 
        input : projectID -> gcp project srting
                bucket_name -> bucket
                blob_path -> path/item.format
                
        output : base64_images -> ["","","",""] PNGbase64 of each page in document
    """
    # Initialize a client with the specified project
    storage_client = storage.Client(project = projectID)

    # Specify the bucket name
    bucket_name = "dev_pun_preprocess"

    # Create a bucket object for our bucket
    bucket = storage_client.get_bucket(bucket_name)
    # Create a blob object from the filepath
    blob_path = "001_จังหวัดปราจีนบุรี_พร้อมเอกสารแนบ.pdf"
    blob = bucket.blob(blob_path)

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

def generate_parallel(base64_images_dict, promt,projectID,projectLocate): #แก้ใส่ ต่ำแหน่งไปด้วย
  """
    this function sent request to LLM in parallel 
    
    input : base64_images_dict -> [{ "no_page" : int , "image" : base64 },,,,,,,]
  
  """
  vertexai.init(project=projectID, location=projectLocate)
  model = GenerativeModel("gemini-1.5-flash-preview-0514")
  
  #base64_images_dict  { "image" : , "no_page" :}  
  promt = promt*len(base64_images_dict)
  # Use a ThreadPoolExecutor for parallel execution
  with ThreadPoolExecutor(max_workers=len(base64_images_dict)) as executor:
    futures = []
    ans = []
    for image_data, image_desc in zip(base64_images_dict, promt):
      future = executor.submit(_generate_image_text, model, image_data, image_desc)
      futures.append(future)

    # Gather results from parallel tasks
    for future in futures:
      result_text = future.result()
      # print(result_text)
      ans.append(result_text)
    return(ans)

def _generate_image_text(model, image_data, image_description):
  base64_images = image_data["image"]
  no_page = image_data["no_page"] 
  
  image = Part.from_data(mime_type="image/png", data=base64.b64decode(base64_images))
  generation_config = {
      "max_output_tokens": 8192,
      "temperature": 0.5,
      "top_p": 0.5,
  }
  responses = model.generate_content(
      [image, image_description],
      generation_config=generation_config,
      stream=True,
  )

  text = ""
  for response in responses:
    text += response.text
  return {str(no_page) : text ,}

# Example usage with multiple images and descriptions
def transform_parallel_input(list_intput): 
    
    """transform list to dict with page_id
        input : [bas64 1 , base64 2, ,,,]
        output :  -> [{ "no_page" : int , "image" : base64 },,,,,,,]
    """
    ans_list = []
    for i,item in enumerate(list_intput): #[{}, {}, {},,, ]
        ans_list.append({"image" : item, "no_page" : i})
    return ans_list

def overlap_page(overlap_size, data_dict):
    overlap_size = 50
    data_dict_overlap = data_dict.copy()
    for no_page in range(1,len(data_dict)-1):
        no_page_pre = str(no_page-1)
        no_page_post = str(no_page+1)
        data_dict_overlap[str(no_page)] = data_dict[no_page_pre][-overlap_size:]+" "+data_dict[str(no_page)] + data_dict[no_page_post][0:overlap_size] 
    data_dict_overlap["0"] = data_dict["0"] + data_dict["1"][0:overlap_size]
    data_dict_overlap[str(len(data_dict)-1)] = data_dict[str( len(data_dict)-2 )][-overlap_size:] + data_dict[str(len(data_dict)-1)]
    return data_dict_overlap

base64_images_input = bucket2PNGbase64(projectID,bucket_name,blob_path)
promt = """หน้าที่ของคุณคือแปลงเอกสารที่ได้รับให้เป็นตัวหนังสือ โดยแบ่งออกเป็นสามกรณีดังนี้:
1. เมื่อพบข้อความในเอกสาร: ให้ใช้เทคโนโลยี OCR เพื่ออ่านข้อความและเขียนให้เหมือนต้นฉบับโดยไม่แก้ไขข้อมูลหรือคำใดเด็ดขาดๆ
2. เมื่อพบรูปภาพในเอกสาร: ให้กำหนดหน้าที่ให้ผู้ใช้อธิบายรูปภาพให้เข้าใจง่ายและชัดเจน ถ้าเป็นกระบวนการบางอย่างให้อธิบายกระบวนการนั้น
3. เมื่อพบตารางหรือข้อมูลอื่นๆในเอกสาร: ให้แปลงข้อมูลให้อยู่ในรูปแบบ CSV เพื่อความสะดวกในการใช้งานต่อไป
"""
response = generate_parallel(transform_parallel_input(base64_images_input), promt = promt, projectID= projectID , projectLocate=projectLocate)

if overlap_size == 0 :
    pass
else :
    response_ = overlap_page(overlap_size = overlap_size , data_dict = response )
    response = response_.copy()



