# import openCV
import cv2
import imutils
# import os for file names
import os
# import asyncio for async running of the processing
import asyncio
import threading
import time


availability = []
videos_location = "some location for videos to be processed"
outputs_location = "some location including the prefix of the output names"
output_videos_location = "in case of video outputs a location for those"
def process_all_videos_dynamic():
    # form a pool of jobs
    print("Forming a pool of jobs...")
    videos_pool_dir = []
    videos_pool_file = []
    videos_dir = videos_location
    for i in range(10, 14):
        processing_dir = videos_dir + str(i)
        file_list = os.listdir(processing_dir)
        for filename in file_list:
            videos_pool_dir.append(processing_dir)
            videos_pool_file.append(filename)

    # initialise synchronisation variables
    n_workers = 64
    counters = []
    threads = []
    for i in range(0, n_workers):
        availability.append(1)
        counters.append(0)
    # while the pool is not empty
    #   pause a minute
    #   for every available worker, assign a task from the pool
    print("Processing the pool of jobs...")
    while len(videos_pool_dir) > 0:
        if (len(threads) < n_workers):
            k = len(threads)
            print("Processing video: " + videos_pool_dir[len(videos_pool_dir) - 1] + "/" + videos_pool_file[len(videos_pool_file) - 1] + " started at: " + time.ctime(time.time()))
            threads.append(threading.Thread(target=work_on_a_video,
                                            args=(videos_pool_dir.pop(-1) + "/", videos_pool_file.pop(-1), k, counters[k],)))
            availability[k] = 0
            threads[k].start()
        else:
            time.sleep(20)
            for i in range(0, n_workers):
                if availability[i] == 1:
                    print("Processing video: " + videos_pool_dir[len(videos_pool_dir) - 1] + "/" + videos_pool_file[
                        len(videos_pool_file) - 1] + " started at: " + time.ctime(time.time()))
                    threads[i] = threading.Thread(target=work_on_a_video,
                                                  args=(videos_pool_dir.pop(-1) + "/", videos_pool_file.pop(-1), i, counters[i],))
                    availability[i] = 0
                    threads[i].start()

    print("Wrapping up all jobs...")
    for k in range(0, len(threads)):
        threads[k].join()

def work_on_a_video(dir_name, file_name, threadno, counter):
    # perform the analysis
    # construct the name of the file
    name = dir_name + file_name

    # Initializing the HOG person
    # detector
    hog = cv2.HOGDescriptor()
    hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())

    cap = cv2.VideoCapture(name)

    # prepare the path for printing the images
    dir_path = outputs_location + str(threadno)
    os.makedirs(dir_path, exist_ok=True)
    base_path = os.path.join(dir_path, file_name)
    digit = len(str(int(cap.get(cv2.CAP_PROP_FRAME_COUNT))))

    #print(str(threadno) + " " + str(counter))
    frame_number = 0
    while cap.isOpened():
        # Reading the video stream
        ret, image = cap.read()

        # print the frame number
        if ret:
            image = imutils.resize(image,
                                   width=min(400, image.shape[1]))

            frame_number = frame_number + 1
            # print(str(threadno) + " " + str(frame_number))

            # Detecting all the regions
            # in the Image that has a
            # pedestrians inside it
            (regions, _) = hog.detectMultiScale(image,
                                                winStride=(4, 4),
                                                padding=(4, 4),
                                                scale=1.05)

            # Drawing the regions in the
            # Image
            for (x, y, w, h) in regions:
                cv2.rectangle(image, (x, y),
                              (x + w, y + h),
                              (0, 0, 255), 2)
                # output the image in the directory
                cv2.imwrite('{}_{}.{}'.format(base_path, str(counter).zfill(digit), "jpg"), image)

            # Showing the output Image
            # cv2.imshow("Image", image)
            if cv2.waitKey(25) & 0xFF == ord('q'):
                break
            counter = counter + 1
        else:
            break

    cap.release()
    cv2.destroyAllWindows()
    print("Processing video: " + name + " finished at: " + time.ctime(time.time()))

    availability[threadno] = 1

    return counter
    # once done, enable the availability

async def process_all_videos_async():
    videos_dir = videos_location
    counters = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    threads = []
    for i in range(9, 17):
        processing_dir = videos_dir + str(i)
        file_list = os.listdir(processing_dir)
        j = 0
        while (j  < len(file_list)):
            for k in range(0, min(len(file_list) - j, 64)):
                print("Processing video: " + processing_dir + "/" + file_list[j] + " started at: " + time.ctime(time.time()))
                if (len(threads) < k + 1):
                    threads.append(threading.Thread(target=process_single_video_async, args=(processing_dir + "/", file_list[j], k, counters[k],)))
                else:
                    threads[k] = threading.Thread(target=process_single_video_async, args=(processing_dir + "/", file_list[j], k, counters[k],))
                # asynio not working #counters[k] = loop.create_task(process_single_video_async(processing_dir + "/", file_list[j], k, counters[k]))
                threads[k].start()
                j = j + 1
            for k in range(0, min(len(file_list) - j, 64)):
                threads[k].join()
            # asynio not working #await asyncio.wait([counters[0], counters[1], counters[2], counters[3], counters[4], counters[5], counters[6], counters[7],
            # asynio not working #                    counters[8], counters[9], counters[10], counters[11], counters[12], counters[13], counters[14], counters[15]])

def process_single_video_async(dir_name, file_name, threadno, counter):
    # construct the name of the file
    name = dir_name + file_name

    # Initializing the HOG person
    # detector
    hog = cv2.HOGDescriptor()
    hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())

    cap = cv2.VideoCapture(name)

    # prepare the path for printing the images
    dir_path = outputs_location + str(threadno)
    os.makedirs(dir_path, exist_ok=True)
    base_path = os.path.join(dir_path, file_name)
    digit = len(str(int(cap.get(cv2.CAP_PROP_FRAME_COUNT))))

    #print(str(threadno) + " " + str(counter))
    frame_number = 0
    while cap.isOpened():
        # Reading the video stream
        ret, image = cap.read()

        # print the frame number
        if ret:
            image = imutils.resize(image,
                                   width=min(400, image.shape[1]))

            frame_number = frame_number + 1
            # print(str(threadno) + " " + str(frame_number))

            # Detecting all the regions
            # in the Image that has a
            # pedestrians inside it
            (regions, _) = hog.detectMultiScale(image,
                                                winStride=(4, 4),
                                                padding=(4, 4),
                                                scale=1.05)

            # Drawing the regions in the
            # Image
            for (x, y, w, h) in regions:
                cv2.rectangle(image, (x, y),
                              (x + w, y + h),
                              (0, 0, 255), 2)
                # output the image in the directory
                cv2.imwrite('{}_{}.{}'.format(base_path, str(counter).zfill(digit), "jpg"), image)

            # Showing the output Image
            # cv2.imshow("Image", image)
            if cv2.waitKey(25) & 0xFF == ord('q'):
                break
            counter = counter + 1
        else:
            break

    cap.release()
    cv2.destroyAllWindows()
    print("Processing video: " + name + " finished at: " + time.ctime(time.time()))

    return counter


def process_all_videos():
    videos_dir = videos_location
    counter = 0
    for i in range(1, 17):
        processing_dir = videos_dir + str(i)
        file_list = os.listdir(processing_dir)
        for file in file_list:
            print("Processing video: " + processing_dir + "/" + file)
            counter = process_single_video(processing_dir + "/", file, counter)

def process_single_video(dir_name, file_name, counter):
    # construct the name of the file
    name = dir_name + file_name

    # Initializing the HOG person
    # detector
    hog = cv2.HOGDescriptor()
    hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())

    cap = cv2.VideoCapture(name)

    # prepare the path for printing the images
    dir_path = output_videos_location
    os.makedirs(dir_path, exist_ok=True)
    base_path = os.path.join(dir_path, file_name)
    digit = len(str(int(cap.get(cv2.CAP_PROP_FRAME_COUNT))))

    while cap.isOpened():
        # Reading the video stream
        ret, image = cap.read()
        if ret:
            image = imutils.resize(image,
                                   width=min(400, image.shape[1]))

            # Detecting all the regions
            # in the Image that has a
            # pedestrians inside it
            (regions, _) = hog.detectMultiScale(image,
                                                winStride=(4, 4),
                                                padding=(4, 4),
                                                scale=1.05)

            # Drawing the regions in the
            # Image
            for (x, y, w, h) in regions:
                cv2.rectangle(image, (x, y),
                              (x + w, y + h),
                              (0, 0, 255), 2)
                # output the image in the directory
                cv2.imwrite('{}_{}.{}'.format(base_path, str(counter).zfill(digit), "jpg"), image)

            # Showing the output Image
            # cv2.imshow("Image", image)
            if cv2.waitKey(25) & 0xFF == ord('q'):
                break
            counter = counter + 1
        else:
            break

    cap.release()
    cv2.destroyAllWindows()
    return counter


# The program starts here
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_all_videos_dynamic())
    loop.close()
