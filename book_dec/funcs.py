import base64
import io

import cv2
import numpy as np
from PIL import Image
import pytesseract

# PSM: https://pyimagesearch.com/2021/11/15/
# tesseract-page-segmentation-modes-psms-explained-how-to-improve-your-ocr-accuracy/
custom_oem_psm_config = r'--oem 3 --psm 6'
# custom_oem_psm_config = r'--oem 3 --psm 11'



def proc_book_image(raw_image):
    # When book is a file address
    # image = Image.open(raw_image)

    image = Image.open(io.BytesIO(raw_image))
    preprocessed_image = preprocess_book_spine(image)
    books_detections = pytesseract.image_to_string(preprocessed_image, lang="heb", config=custom_oem_psm_config)
    books = post_detection_process(books_detections)
    return books


def preprocess_book_spine(image):
    """
    Preprocesses an image of a book spine for OCR.

    Args:
        image: The input image as a NumPy array.

    Returns:
        A preprocessed image ready for OCR.
    """
    image_data = np.asarray(image)
    # 1. Grayscale and Noise Reduction
    gray = cv2.cvtColor(image_data, cv2.COLOR_BGR2GRAY)
    # https://docs.opencv.org/4.x/d4/d86/group__imgproc__filter.html#ga9d7064d478c95d60003cf839430737ed
    filtered = cv2.bilateralFilter(gray, 15, 5, 75)

    # 2. Image Enhancement
    enhanced = cv2.equalizeHist(filtered)
    clahe = cv2.createCLAHE(clipLimit=1.0, tileGridSize=(64, 64))
    enhanced = clahe.apply(enhanced)

    _, binary = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)

    # 5. Normalization
    normalized = Image.fromarray(binary)#.rotate(90)#.resize((256, 64), resample=ImageFilter.LANCZOS)  # Adjust size as needed

    return normalized


def post_detection_process(books):
    new_books = []
    for book in books.split("\n"):
        book = "".join([ch for ch in book if ch.isalnum() or ch == " "])
        if len(book) > 2:
            new_books.append(book[::-1])
    return new_books

