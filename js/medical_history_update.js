function reformatDxFieldLabel(divName) {
	var labelDivHTML = "";
	var labelDiv = document.getElementById(divName);
	var dxDateRule = labelDiv.getElementsByClassName("dx_date_rule")[0].childNodes[0].innerHTML;
	var dxAge = Number(labelDiv.getElementsByClassName("dx_age")[0].childNodes[0].innerHTML);
	// test dx date rule
    switch(dxDateRule) {
    	case "Date":
			var dxDate = labelDiv.getElementsByClassName("dx_date")[0].childNodes[0].innerHTML;
    		labelDivHTML = "The actual dx date is " + dxDate + ", age " + dxAge.toFixed(2) + ".";
            break;
  		case "Age":
    		labelDivHTML = "Estimated dx at age " + dxAge.toFixed(1);
			break;
		case "Month":
			var dxDateEst = labelDiv.getElementsByClassName("dx_date_est")[0].childNodes[0].innerHTML;
			var dxDateEstArray = dxDateEst.split("-");
			var dxYear = dxDateEstArray[0];
			var dxMonth = Number(dxDateEstArray[1]);
			dxMonth = dxMonth - 1;
			var dxDay = dxDateEstArray[2];
			var dxDateObj = new Date(dxYear, dxMonth, dxDay);
			var dxMonthLong = dxDateObj.toLocaleString('default', { month: 'long' });
			labelDivHTML = "Estimated dx at " + dxMonthLong + " " + dxYear + ", age " + dxAge.toFixed(2);
			break;
		case "Clinic dx":
			var dxDateEst = labelDiv.getElementsByClassName("dx_date_est")[0].childNodes[0].innerHTML;
			var dxDateEstArray = dxDateEst.split("-");
			var dxYear = dxDateEstArray[0];
			var dxMonth = Number(dxDateEstArray[1]);
			dxMonth = dxMonth - 1;
			var dxDay = dxDateEstArray[2];
			var dxDateObj = new Date(dxYear, dxMonth, dxDay);
			var dxMonthLong = dxDateObj.toLocaleString('default', { month: 'long' });
			labelDivHTML = "Clinic dx at the " + dxYear + " clinic (" + dxMonthLong + " " + dxYear + "), age " + dxAge.toFixed(2);
			break;
		case "Dx age unknown":
			labelDivHTML = "Diagnosis present, but age unknown";
			break;
		case "No dx":
			labelDivHTML = "No diagnosis";
			break;
  		default:
    		// code block
	}
    labelDiv.innerHTML = labelDivHTML;
}
window.addEventListener('load', function(){reformatDxFieldLabel("wfs_dx_label")})
window.addEventListener('load', function(){reformatDxFieldLabel("hearloss_dx_label")})