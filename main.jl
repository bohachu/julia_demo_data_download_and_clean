using CSV
using DataFrames
using ZipFile
using CodecZlib
using Dates
using GZip
using HTTP
using JSON
import Base


function download_file_from_google_drive(google_file_id::String, output_file::String)
    # 呼叫 Linux shell script (google_drive_downloader.sh) 並傳入參數
    run(`./google_drive_downloader.sh $google_file_id $output_file`)
end


# 001 解壓縮 zip 檔案
function unzip(file,exdir="")
    fileFullPath = isabspath(file) ?  file : joinpath(pwd(),file)
    basePath = dirname(fileFullPath)
    outPath = (exdir == "" ? basePath : (isabspath(exdir) ? exdir : joinpath(pwd(),exdir)))
    isdir(outPath) ? "" : mkdir(outPath)
    zarchive = ZipFile.Reader(fileFullPath)
    for f in zarchive.files
        fullFilePath = joinpath(outPath,f.name)
        if (endswith(f.name,"/") || endswith(f.name,"\\"))
            mkdir(fullFilePath)
        else
            write(fullFilePath, read(f))
        end
    end
    close(zarchive)
end

function zipcsv(csv_filename)
    # 要壓縮的 csv 檔案名稱
    #csv_filename = "data.csv"

    # 設定壓縮後的檔案名稱
    zip_filename = "$csv_filename.gz"

    input_file = csv_filename
    output_file = zip_filename

    # 讀取並壓縮 CSV 文件
    #println("-------01 reading")
    data = read(input_file, String)
    #println("-------01 writing = $output_file")
    stream = GZip.open(output_file, "w")
    write(stream, data)
    close(stream)
    #println("-------03 writing done!")

    # 刪除原始 CSV 文件
    rm(input_file)

end

# 讀取 CSV 檔案，回傳 DataFrame
function read_csv(filename)
    return CSV.read(filename, DataFrame; dateformat="yyyy-mm-dd HH:MM:SS.u")
end

# 篩選出日期部分，新增 DATE 欄位
function add_date_column(df)
    # get the first 10 characters of field: TIME 
    df.DATE = [string(SubString(string(x), 1, 10)) for x in df.TIME]
    return df
end

# 新增 sensor_id 欄位
function add_sensor_id(df, sensor_id)
    df[!, "SENSOR_ID"] .= sensor_id
    return df
end

# 取得不重複的 DEVICE_ID 與 DATE 清單
function get_unique_values(df)
    device_ids = unique(df.DEVICE_ID)
    dates = unique(df.DATE)
    return device_ids, dates
end

# 檢查目錄是否存在，不存在則建立
function check_directory_exists(directory)
    if !isdir(directory)
        mkdir(directory)
    end
end

# 檢查檔案是否存在
function is_file_exists(zip_filename)
    return isfile(zip_filename)
end

# 分別儲存每個 DEVICE_ID 的 data frame 成獨立的檔案
function save_device_dataframes(df, device_ids, dates, outputdir="results")
    for device_id in device_ids
        for date in dates
            device_df = filter(row -> row.DEVICE_ID == device_id && row.DATE == date, df)
            if !isempty(device_df)
                # 將 csv 檔案清洗成新的檔案格式
                device_df.createTime = device_df.TIME
                selected_columns = [:createTime, :DEVICE_ID, :LAT, :TIME, :LON, :SENSOR_ID, :VALUE]
                new_df = device_df[:, selected_columns]
                #show(new_df)
                #rename!(new_df, [:DEVICE_ID, :LAT, :TIME, :LON, :SENSOR_ID, :VALUE] => [] )
                rename!(new_df, :DEVICE_ID => :deviceId, :LAT => :lat, :TIME => :localTime, :LON => :lon, :SENSOR_ID => :sensorId, :VALUE => :value )

                check_directory_exists(outputdir)
                directory = "./$outputdir/$device_id"
                check_directory_exists(directory)
                filename = "$directory/$device_id-$(string(date)).csv"
                zip_filename = "$directory/$device_id-$(string(date)).csv.gz"
                tmp_csv_filename = "$directory/$device_id-$(string(date))_tmp.csv"
                tmp_zip_filename = "$directory/$device_id-$(string(date))_tmp.csv.gz"
                
                if is_file_exists(zip_filename)
                    # 000001 檔案已經存在了，要附加上去
                    #println("000001 File exists at the given path.")

                    CSV.write(tmp_csv_filename, new_df)
                    zipcsv(tmp_csv_filename)                    

                    # 先開啟目標檔案
                    open(tmp_zip_filename, "r") do io
                        reader = GzipDecompressorStream(io)
                        header = true
                        for line in eachline(reader)
                            # 跳過 df2 的 header
                            if header
                                header = false
                            else
                                # 將 tmp_zip_filename 的內容添加至 zip_filename
                                open(zip_filename, "a") do io2
                                    writer = GzipCompressorStream(io2)
                                    write(writer, line)
                                    write(writer, "\n")  # 新行
                                    close(writer)
                                end
                            end
                        end
                        close(reader)
                    end

                    # 刪除暫存檔 tmp_csv_filename
                    rm(tmp_zip_filename)

                else
                    # 000002 第一次產生檔案                    
                    #println("000002 File does not exist at the given path.")
                    CSV.write(filename, new_df)
                    zipcsv(filename)    
                end

            end
        end
    end
end

function download_zip_file(file_id, destination)
    download_file_from_google_drive(file_id, destination)
end


function find_sensor_id(filename::String)
    sensor_list = JSON.parsefile("sensor_list.json")

    for sensor_id in sensor_list
        if occursin(sensor_id, filename)
            return sensor_id
        end
    end
    return ""
end


# 主程式
function main(args)
    # 檢查是否提供了足夠的參數
    if length(args) < 2
        println("請提供 file_id 和 zipfilename")
        println(""" ussage: julia main.jl "google-drive-file-id" "test_filename_from_google_drive.zip" """)
        exit(1)
    end

    # 分別將參數指定給對應的變數
    file_id = args[1]
    zipfilename = args[2]
    foldername = replace(zipfilename, ".zip" => "")

    # init settings
    println("file_id: $file_id")
    println("zipfilename: $zipfilename")


    #000 Download zip file
    download_zip_file(file_id, zipfilename)   
    print("[info] download_zip_file=$zipfilename.....Done!")

    ##Base.exit() # debug

    #001 unzip
    println("[info] 001 unzip file...")
    unzip(zipfilename, foldername)
    println("[info] 002 unzip file... done!")



    #002
    flag_loop = 1
    for csv_file in readdir(foldername)
        if endswith(csv_file, ".csv")
            
            # skip this system file..
            if csv_file == "empty_data_log.csv" 
                continue
            end
            if csv_file == "metedata.csv" 
                continue
            end
            
            sensor_id = find_sensor_id(csv_file) #Find sensor_id in filename from list
            
            # Skip if no sensor_id detected in filename
            if sensor_id == ""
                continue
            end

            println("[info] 003 filename=$csv_file and sensorid=$sensor_id")

            #003 read csv to dataframe
            input_csv = joinpath(foldername, csv_file)
            df = read_csv(input_csv)

            #004 add sensor_id
            df = add_sensor_id(df, sensor_id)
            
            #005 add date
            df = add_date_column(df)
            #show(df)
            
            device_ids, dates = get_unique_values(df)

            #006 save all device + date to csv files
            println("006 start save_device_dataframes")

            save_device_dataframes(df, device_ids, dates)

        end
    end
end


start_time = now()  # 記錄程式開始時間
# 呼叫 main 函數並將 ARGS 作為參數傳遞
main(ARGS)
end_time = now()  # 記錄程式結束時間

elapsed_time = end_time - start_time  # 計算主程式執行時間
println("Executing Time:", elapsed_time)

