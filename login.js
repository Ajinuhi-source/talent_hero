async function login(page, email, password) {
    await page.goto('https://app.ahrefs.com/user/login');
    console.log(page.url());
  
    await page.waitForTimeout(2000);
  
    await page.type("input[name='email']", email);
    console.log('Inputted email');
  
    await page.waitForTimeout(2000);
    console.log('waited 2 seconds');
  
    await page.type("input[name='password']", password);
    console.log('Inputted password');
  
    await page.waitForTimeout(2000);
  
    await page.keyboard.press('Enter');
    console.log('Pressed enter button to log in');
  
    await page.waitForTimeout(5000);
  }
  
  module.exports = login;