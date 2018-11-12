#coding=utf-8

def arguments(sys_argv):

    job_name = sys_argv[0]
    start_date = None
    end_date = None

    for argv in sys_argv[1:]:

        argv_split = argv.split("=")
        if  "start_date" == argv_split[0]:
            start_date = argv_split[1]
        elif  "end_date" == argv_split[0]:
            end_date = argv_split[1]

    return {"job_name":job_name,
            "start_date":start_date,
            "end_date":end_date}
