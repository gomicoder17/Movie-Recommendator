Movie Recommendator using Airflow and Docker

Each minute, the script looks into io/input.txt, extracts the movie or series and makes a recommendation in
the io/output.txt file.

The input.txt format is:
    <movie|series>,<title>

To get it running, execute the command:
    docker-compose up