import yaml
import json
import re

default_bq_profile = {
    "config": {
        "partial_parse": True,
        "printer_width": 120,
        "send_anonymous_usage_stats": False,
        "use_colors": True
    },
    "normalize": {
        "outputs": {
            "prod": {
                "dataset": "",
                "keyfile_json": {},
                "location": "",
                "method": "service-account-json",
                "priority": "interactive",
                "project": "",
                "retries": 3,
                "threads": 8,
                "type": "bigquery"
            }
        },
        "target": "prod"
    }
}

if __name__ == "__main__":
    print("Reading destination config data and parsing...")
    with open("destination_config.json") as f:
        dest_conf = json.loads(f.read())
    
    profile_vars = {
        "dataset": dest_conf["dataset_id"],
        "location": dest_conf["dataset_location"],
        "project": dest_conf["project_id"],
        "keyfile_json": json.loads(dest_conf['credentials_json'])
    }
    default_bq_profile["normalize"]["outputs"]["prod"].update(profile_vars)
    
    print("Writing dbt profile to execute...")
    with open("profiles.yml", "w") as f:
        f.write(yaml.dump(default_bq_profile))

    print("Checking if need to fix --project-dir if not pointing to /config/git_repo...")
    with open("dbt_config.json") as f:
        dbt_config = json.loads(f.read())

    if "--project-dir" in dbt_config["dbtArguments"]:
        print("Project dir setted in arguments... checking correct path...")
        dbt_args = dbt_config["dbtArguments"]
        pattern = r"(?<=--project-dir )(.*?)(?=[\b|\s]|$)"
        found = re.findall(pattern, dbt_args)[0]
        if "/config/git_repo" not in found:
            dbt_config['dbtArguments'] = re.sub(pattern, r"/config/git_repo/{}".format(found), dbt_args)
        
        with open("dbt_config.json", "w") as f:
            f.write(json.dumps(dbt_config))

    else:
        print("Nothing to fix...")
    