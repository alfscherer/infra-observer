Example credential files for local development. Copy to `secrets/` (git-ignored):

    cp -r secrets.example secrets

A credential reference such as `snmp/lab` resolves to `secrets/snmp/lab.json`, or
to the environment variable `INFRA_OBSERVER_SECRET_SNMP_LAB` (a JSON object).
The values here belong to the simulator only. See OPERATIONS.md for how a real
secret store plugs in.
