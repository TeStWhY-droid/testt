# Lab2: Malware Detection Using Wazuh

Malware is malicious software. It is installed on a computer without the user's permission. Malware can be used to encrypt (Ransomware), steal (info stealer), and spy on users. Malware detection is the process of detecting and analyzing suspicious software and files on systems and networks. Most security products detect malware by using previous malware signatures to match and detect it, or analyze malicious behavior. There are sophisticated malwares that can evade the detection system by using multiple techniques when it enters the system. Wazuh uses different techniques and methods to detect malicious files. We will learn about those methods and integrating third-party tools to help Wazuh improve malware detection.

## Types of Malware

1. **Viruses** → Malware that attaches itself to files and programs, then spreads by infecting other files. Can cause damage by corrupting data (ILOVEYOU virus).
2. **Worms** → Malware that copies or replicates itself and spreads via networks by exploiting security holes to infect other connected systems (Blaster worm).
3. **Trojans** → Malicious software that looks like legitimate files or programs, lets cybercriminals enter without needing permissions. In other words, malicious content inside legitimate software (ZEUS steals financial info).
4. **Ransomware** → Malware that encrypts all data existing on a system to get paid to decrypt them. One of the most high-severity malwares (WannaCry and Locky).
5. **Spyware** → Malware designed for spying by covertly monitoring and collecting info from infected systems, like sensitive info, passwords, and browsing habits (CoolWebSearch via pop-up ads, FinSpy).
6. **Rootkits** → Malware that gets privileged access to a system without being noticed, hides attacker presence and keeps control of the infected system (Alureon, BMG Rootkit).

Malware is spread via different ways like phishing emails, malicious downloads, infected websites, and physical devices like USBs. Cybercriminals are always developing their techniques to exploit new vulnerabilities using malware, which makes security solutions also keep developing to deal with sophisticated malware.

## Wazuh Malware Detection Methods

Wazuh offers several methods to detect malware, by combination of log analysis, intrusion detection, and threat intelligence. It also provides the ability to execute custom scripts for automated reaction activities. Here are some of Wazuh's methods for malware detection:

### 1. Threat Detection Rules and File Integrity Monitoring (FIM)
In this method, Wazuh uses its built-in capability to detect any critical file modification. Here are some of those capabilities:

- **1.1.** Set of predefined continuously monitored threat detection principles (to identify suspicious activities, events, patterns that may lead to malware infections or security breaches).
- **1.2.** FIM monitors modifications to files and directories. Wazuh generates an alert when unauthorized changes occur.

### 2. Rootkit Behavior Detection
Using rootcheck function to detect anomalies that might indicate malware in an endpoint, like:

- **2.1.** Rootkits are a form of malware that can hide their presence and malicious actions on a system. Wazuh identifies rootkit-like activities using behavior-based detection techniques.
- **2.2.** Wazuh searches for any attempts of privilege escalation or hiding files and processes, and then it generates an alert.

### 3. Third Party: VirusTotal Integration
Wazuh detects malicious files via integration of VirusTotal:

- **3.1.** Is a web-based service that scans files and URLs for potential malware using different antivirus engines and multiple threat intelligence sources.
- **3.2.** Wazuh catches a file or URL that is suspected and sends it to VirusTotal to scan it with multiple antivirus engines. Then Wazuh alerts the findings of VirusTotal. The confidence is increased if the file or URL is detected multiple times with multiple antivirus engines.

### 4. Third Party: YARA Integration
Wazuh detects malware samples using YARA, which is an open-source tool that identifies and classifies malware artifacts based on their binary patterns:

- **4.1.** YARA is a powerful tool that lets you write your own rules to find malware and certain patterns in files and processes.
- **4.2.** Can use YARA integration to create custom signatures that detect specific malware strains or behaviors that are not covered by the normal Wazuh rules.

## Malware Detection Using FIM

When a system gets compromised by malware, it may create new files or edit existing files like:

1. Executable files (.exe, .dll, .bat, and .vbs)
2. Configuration files (.cfg and .ini)
3. Temporary files (.tmp)
4. Registry entries
5. Log files (.log)
6. Payload files
7. Hidden files and directories
8. Batch scripts (.bat)
9. PowerShell (.ps1)
10. Specially crafted documents with a malicious payload (.doc, .xls, and .pdf)

Using this information, we can create an FIM rule in Wazuh to detect any file changes. However, we will get a high number of false positive alerts. To solve this problem, we can focus on specific directories.

Now we will learn how to create Wazuh rules to detect some common malware patterns.

## First Case: Configuring and Testing FIM

FIM is technology that monitors the integrity of system and application files. It guards sensitive data, apps, and device files by monitoring, scanning, and confirming their integrity. It detects the changes on critical files in the network. Because Wazuh is open source (OSSEC), it has open source FIM.

OSSEC (Open Source HIDS Security) is open source, free host-based intrusion detection system. When a user or process creates, modifies, or deletes a monitored file, the Wazuh FIM module generates an alert. By default, the FIM is enabled on the Wazuh agent. 

The following are the steps to test FIM:

1. We need Wazuh manager → Ubuntu
2. We need Wazuh agent → purple
3. FIM module configuration file is present in the `<syscheck>` tag under the ossec.conf file located in (`/var/ossec/etc/ossec.conf`)
4. We only need to add the directories to be monitored under `<syscheck>` tag. The following configuration will monitor specified files and directories for any types of changes or modifications:

### Configuration Steps:

- **4.1.** Inside (`/var/ossec/etc/ossec.conf`) we will search for `<syscheck>` tag
- **4.2.** Inside `<syscheck>` tag we will find `<frequency>` tag which represents the frequency that `<syscheck>` tag is executed. By default, we will put inside this tag the number of seconds. In our case is 43200s which is every 12 hours
- **4.3.** `<disabled>` tag to enable and disable FIM. In our case we will put "no" to enable it
- **4.4.** `<directories>` tag identifies the directories or files to be monitored. It takes different attributes, like:
  - **4.4.1.** `realtime` → to monitor the directory or file in real time
  - **4.4.2.** `whodata` → to monitor the directory or file in real time but add more data than realtime like:
    - **4.4.2.1.** The user who has edited the file
    - **4.4.2.2.** And the file name that has been edited
  - **4.4.3.** `report_changes` → appears the changes that happened. It appears the content before and content after changes

Example: `<directories whodata="yes" report_changes="yes">/home/purple</directories>` - we monitor here the `/home/purple` directory

We can see:
- `decoder.name` : -- : `syscheck_integrity_changed` → This field represents a new entry related to system checks
- `full_log` : -- : `File '/home/purple/FIMTEST' modified` → represents that file FIMTEST has been modified

- **4.5.** `<ignore>` tags indicate files or directories to ignore during the monitoring process

## Second case: Detecting suspicious files in the PHP server using the FIM module

PHP is known for its simplicity, speed, and flexibility. Currently, there are a very huge number of websites using PHP. We can find PHP files inside those directories (`/var/www/html/`, `/var/www/public_html/`, root directory). Now we will test malware using FIM module in PHP server, steps:

1. Wazuh manager --> Ubuntu
2. Wazuh agent has PHP --> purple
3. We will create a Wazuh rule to detect file creation and modification on the PHP server
4. We will add different types of PHP file extensions under the `<field>` tag of the Wazuh rule
5. We will go to Server management in navigation bar of Wazuh dashboard
6. We will choose rules then choose add new rules file
7. We will name the rule (custom_fim.xml)
8. This is the rule:

```xml
<group name="linux, webshell, windows,">
    <!-- Detect suspicious web-shell file creation -->
    <rule id="100500" level="12">
        <if_sid>554,550</if_sid>
        
        <field name="file" type="pcre2">(?i)\.(php|php[3-8]
        |phtml|phps|phar|asp|aspx|jsp|cshtml|vbhtml)$</field>
        
        <description>[File creation]: Possible web shell scripting file ($(file)) 
        created</description>
    </rule>
</group>
```

   8.1. `<if_sid>554</if_sid>`: This tag represents a list of rule IDs.
   
   8.2. `<field>` tag is used as a requisite (condition) to trigger the rule. In this case, the content is the list of all possible PHP file extensions.

9. To test our FIM rule, we will add a new file called antivirusupdate.php in `/home/purple`:
   
   9.1. `touch /home/purple/antivirusupdate.php`
   
   9.2. Alert: `[File creation]: Possible web shell scripting file (/home/purple/antivirusupdate.php) created`
   
        9.2.1. rule.id: 100500

This FIM rule may lead to a lot of false positive alerts on the Wazuh dashboard. To overcome this situation, you can fine-tune your `<syscheck>` block by adding more `<ignore>` tags.

## Third case: The CDB list

The CDB list is a repository that has distinct hashes or checksums of malicious/benign files. Wazuh makes comparison of any file hash and hashes in CDB list. The CDB list consists of lists of users, file hashes, IP addresses, domain names, and so on.

You can save a list of users, file hashes, IP addresses, and domain names in a text file called a CDB list. A CDB list can have entries added in a key:value pair or a key:only format. Lists in CDBs can function as allow or deny lists. Wazuh processes the CDB list like here:

1. **Hash generation**: Has both good and bad hashes like IP addresses, malware hashes and domain names. A hash is a unique fixed-length value generated based on the CDB list content.
2. **File comparison**: Wazuh computes file hashes during scanning and compares them with those inside CDB list.
3. **Identification**: Wazuh marks file as malicious if its hash is found inside CDB list.
4. **Alerts and reactions**: Based on set policies, Wazuh has the ability to trigger alerts or responses upon detection.

Now we are setting the CDB list inside Wazuh server (Ubuntu) with malware hashes and create the required rules to trigger alerts if hash matches another in CDB comparison process:

1. **Create a file in the CDB list**: CDB lists are stored in (`/var/ossec/etc/lists`) on Wazuh server. Now we will add new CDB list with name malware-hashes using this command: `nano /var/ossec/etc/lists/malware-hashes`

2. **Adding malware hashes**: Now we will enter known malware hashes in format (key:value pair) where key is actual malware hash and value is name or keyword. One of popular sources for malware hashes is a list published by Nextron Systems, download via (https://github.com/Neo23x0/signature-base/blob/master/iocs/hash-iocs.txt). For testing we will use a few popular malware hashes like Mirai and Fanny by putting inside `/var/ossec/etc/lists/malware-hashes` file:
   
   2.1. `e0ec2cd43f71c80d42cd7b0f17802c73:mirai`
   
   2.2. `55142f1d393c5ba7405239f232a6c059:Xbash`
   
   2.3. `F71539FDCA0C3D54D29DC3B6F8C30E0D:fanny`

3. **Adding the CDB list under default ruleset**: Inside the configuration file (`/var/ossec/etc/ossec.conf`) in `<ruleset>` tag we put the path of hashes files, by adding this line inside the tag: `<list>etc/lists/malware-hashes</list>`

4. **Writing a rule to compare hashes**: Create custom rule in Wazuh server (Ubuntu) inside (`/var/ossec/etc/rules/local_rules.xml`) or from the Wazuh dashboard by going to Server management tab then Rules tab then searching for local_rules.xml then press edit and add this:

```xml
<group name="malware">
    <rule id="110002" level="13">
        <if_sid>554, 550</if_sid>
        <list field="md5" lookup="match_key">etc/lists/malware-hashes</list>
        <description>
            Known Malware File Hash is detected: $(file)
        </description>
        <mitre>
            <id>T1204.002</id>
        </mitre>
    </rule>
</group>
```

   When Wazuh finds a match between the MD5 hash of a recently created or updated file and a malware hash in the CDB list, this rule triggers. When an event occurs that indicates a newly created or modified file exists, rules 554 and 550 will be triggered.

5. **Restart the manager**: We have to restart the Wazuh manager to apply the changes using: `systemctl restart wazuh-manager`

We have successfully created a CDB list of malware hashes and security rules to compare it with the hash of each file in the Wazuh agent. Now in our Wazuh agent we will edit configuration file (`/var/ossec/etc/ossec.conf`) to set our agent to detect file changes in specified directory like we have put in our configuration of FIM (`<directories whodata="yes" report_changes="yes" check_all="yes">/home/purple/malware-tests</directories>`). We will work on this directory (`/home/purple/malware-tests`) to test our cases, then we will restart Wazuh agent using `systemctl restart wazuh-agent`.
