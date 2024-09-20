from typing import List, Optional, Tuple

import cv2
import numpy as np
from loguru import logger
from PIL import Image
from pytesseract import Output, pytesseract

from mapadroid.ocr.screen_type import ScreenType

screen_texts: dict = {1: ['Geburtdatum', 'birth.', 'naissance.', 'date'],
                      2: ['ZURUCKKEHRENDER', 'ZURÜCKKEHRENDER', 'GAME', 'FREAK', 'SPIELER'],
                      3: ['Google', 'Facebook'],
                      4: ['Benutzername', 'Passwort', 'Username', 'Password', 'DRESSEURS'],
                      5: ['Authentifizierung', 'fehlgeschlagen', 'Unable', 'authenticate',
                          'Authentification', 'Essaye'],
                      6: ['RETRY', 'TRY', 'DIFFERENT', 'ACCOUNT', 'ANDERES', 'KONTO', 'VERSUCHEN',
                          'AUTRE', 'AUTORISER'],
                      7: ['incorrect.', 'attempts', 'falsch.', 'gesperrt'],
                      8: ['Spieldaten', 'abgerufen', 'lecture', 'depuis', 'server', 'data'],
                      9: ['hardware', 'requirements'],
                      12: ['Events,', 'Benachrichtigungen', 'Einstellungen', 'events,', 'offers,',
                           'notifications', 'évenements,', 'evenements,', 'offres'],
                      14: ['kompatibel', 'compatible', 'OS', 'software', 'Gerät',
                           'Betriebssystem', 'logiciel'],
                      15: ['continuer...', 'aktualisieren?', 'now?', 'Aktualisieren', 'Aktualisieren,',
                           'aktualisieren', 'update', 'continue...', 'Veux-tu', 'Fais', 'continuer'],
                      16: ['modified', 'client', 'Strike', 'suspension', 'third-party',
                           'modifizierte', 'Verstoß', 'Sperrung', 'Drittpartei'],
                      17: ['Suspension', 'suspended', 'violating', 'days', ],
                      18: ['Termination', 'terminated', 'permanently'],
                      19: ['geplanter', 'Wartungsarbeiten', 'maintenance.', 'maintenance'],
                      21: ['GPS', 'signal', 'GPS-Signal', '(11)', 'introuvable.',
                           'found.', 'gefunden.', 'Signal', 'geortet', 'detect', '(12)'],
                      # Used to be commented since blurry login may result in wrongfully thinking
                      # NEW PLAYER screen reached?
                      23: ['CLUB', 'KIDS'],
                      25: ['SIGNOUT', 'SIGN', 'ABMELDEN', '_DECONNECTER'],
                      30: ['Welcome!', 'Willkommen!', 'Bienvenue!'],
                      31: ['Terms', 'Service'],
                      32: ['PRIVACY', 'DATENSCHUTZ', 'CONFIDENTIALITE'],
                      33: ['Willow.'],
                      34: ['nearby!', 'Nahe!', 'pres'],
                      35: ['Congratulations!', 'Gluckwunsch!', 'felicitations!', 'first', 'erstes', 'abord'],
                      36: ['Sync'],
                      37: ['PokéStops.'],
                      38: ['limitations']
                     }


def screendetection_get_type_internal(image,
                                      identifier) -> Optional[Tuple[ScreenType, Optional[dict], int, int, int]]:
    with logger.contextualize(identifier=identifier):
        returntype: ScreenType = ScreenType.UNDEFINED
        globaldict: Optional[dict] = {}
        diff: int = 1
        logger.debug("__screendetection_get_type_internal: Detecting screen type")

        texts = []
        try:
            with Image.open(image) as frame_org:
                width, height = frame_org.size

                logger.debug("Screensize: W:{} x H:{}", width, height)

                if width < 1080:
                    logger.info('Resize screen ...')
                    frame_org = frame_org.resize([int(2 * s) for s in frame_org.size], Image.LANCZOS)
                    diff: int = 2

                texts = [frame_org]
                for thresh in [200, 175, 150]:
                    fn = lambda x: 255 if x > thresh else 0  # noqa: E731
                    frame = frame_org.convert('L').point(fn, mode='1')
                    texts.append(frame)
                for text in texts:
                    try:
                        globaldict = pytesseract.image_to_data(text, output_type=Output.DICT, timeout=40,
                                                               config='--dpi 70')
                    except Exception as e:
                        logger.error("Tesseract Error: {}. Exception: {}", globaldict, e)
                        globaldict = None
                    logger.debug("Screentext: {}", globaldict)
                    if globaldict is None or 'text' not in globaldict:
                        continue
                    n_boxes = len(globaldict['text'])
                    for index in range(n_boxes):
                        if returntype != ScreenType.UNDEFINED:
                            break
                        if len(globaldict['text'][index]) > 3:
                            for screen_elem in screen_texts:
                                heightlimit = 0 if (screen_elem == 21 or screen_elem == 30) else height / 4
                                if globaldict['top'][index] > heightlimit and globaldict['text'][index] in \
                                        screen_texts[screen_elem]:
                                    returntype = ScreenType(screen_elem)
                    if returntype != ScreenType.UNDEFINED:
                        break

                del texts
                frame.close()
        except (FileNotFoundError, ValueError) as e:
            logger.error("Failed opening image {} with exception {}", image, e)
            return None

        return returntype, globaldict, width, height, diff


def check_pogo_mainscreen(filename, identifier) -> bool:
    with logger.contextualize(identifier=identifier):
        logger.debug("__internal_check_pogo_mainscreen: Checking close except nearby with: file {}", filename)
        try:
            screenshot_read = cv2.imread(filename)
        except Exception:
            logger.error("Screenshot corrupted")
            logger.debug("__internal_check_pogo_mainscreen: Screenshot corrupted...")
            return False
        if screenshot_read is None:
            logger.error("__internal_check_pogo_mainscreen: Screenshot corrupted")
            return False

        height, width, _ = screenshot_read.shape
        gray = screenshot_read[int(height) - int(round(height / 5)):int(height),
               0: int(int(width) / 4)]
        del screenshot_read
        _, width_, _ = gray.shape
        radius_min = int((width / float(6.8) - 3) / 2)
        radius_max = int((width / float(6) + 3) / 2)
        gaussian = cv2.GaussianBlur(gray, (3, 3), 0)
        del gray
        canny = cv2.Canny(gaussian, 200, 50, apertureSize=3)
        del gaussian
        circles = cv2.HoughCircles(canny, cv2.HOUGH_GRADIENT, 1, width / 8, param1=100, param2=15,
                                   minRadius=radius_min,
                                   maxRadius=radius_max)
        del canny
        avatar_likely_present = False
        total_amount_of_circles = 0
        if circles is not None:
            circles = np.round(circles[0, :]).astype("int")
            total_amount_of_circles = len(circles)
            for (pos_x, _, _) in circles:
                if pos_x < width_ - width_ / 3:
                    avatar_likely_present = True
        del circles
        if avatar_likely_present and total_amount_of_circles == 1:
            logger.debug("Found avatar.")
            return True
        logger.warning("Not on mainscreen (avatar {}, amount of circles: {}", avatar_likely_present,
                       total_amount_of_circles)
        return False


def most_frequent_colour_internal(image, identifier, y_offset: int = 0) -> Optional[List[int]]:
    with logger.contextualize(identifier=identifier):
        logger.debug("most_frequent_colour_internal: Reading screen text")
        try:
            with Image.open(image) as img:
                w, h = img.size
                left = 0
                top = int(h * 0.05)
                right = w
                bottom = h - y_offset
                img = img.crop((left, top, right, bottom))
                w, h = img.size
                pixels = img.getcolors(w * h)
                most_frequent_pixel = pixels[0]

                for count, colour in pixels:
                    if count > most_frequent_pixel[0]:
                        most_frequent_pixel = (count, colour)

                logger.debug("Most frequent pixel on screen: {}", most_frequent_pixel[1])
        except (FileNotFoundError, ValueError) as e:
            logger.error("Failed opening image {} with exception {}", image, e)
            return None

        return most_frequent_pixel[1]


def get_screen_text(screenpath: str, identifier) -> Optional[dict]:
    with logger.contextualize(identifier=identifier):
        returning_dict: Optional[dict] = {}
        logger.debug("get_screen_text: Reading screen text")

        try:
            with Image.open(screenpath) as frame:
                frame = frame.convert('LA')
                try:
                    returning_dict = pytesseract.image_to_data(frame, output_type=Output.DICT, timeout=40,
                                                               config='--dpi 70')
                except Exception as e:
                    logger.error("Tesseract Error: {}. Exception: {}", returning_dict, e)
                    returning_dict = None
        except (FileNotFoundError, ValueError) as e:
            logger.error("Failed opening image {} with exception {}", screenpath, e)
            return None

        if isinstance(returning_dict, dict):
            return returning_dict
        else:
            logger.warning("Could not read text in image: {}", returning_dict)
            return None
