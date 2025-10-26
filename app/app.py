from flask import Flask, render_template, request
import subprocess
import os

app = Flask(__name__)
UPLOAD_FOLDER = '/tmp/uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file:
        file_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(file_path)

        # Put file in HDFS
        subprocess.run(f"hdfs dfs -mkdir -p /input", shell=True)
        subprocess.run(f"hdfs dfs -put -f {file_path} /input/", shell=True)

        # Run wordcount MapReduce job
        output_dir = "/output"
        subprocess.run(f"hdfs dfs -rm -r {output_dir}", shell=True)  # Clean old output
        subprocess.run(
            f"hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar wordcount /input/{file.filename} {output_dir}",
            shell=True
        )

        # Read output
        result = subprocess.check_output(f"hdfs dfs -cat {output_dir}/part-r-00000", shell=True).decode()
        return render_template('index.html', result=result)

    return "No file uploaded", 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
